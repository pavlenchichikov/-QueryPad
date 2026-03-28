"""Local ML model for SQL generation — learns from user query history.

Architecture:
  1. Stores every successful (question, schema, sql) pair
  2. TF-IDF vectorizes questions
  3. On new question: finds most similar past questions via cosine similarity
  4. Adapts retrieved SQL to current schema using template substitution
  5. Gets smarter with every query the user runs

Zero external API calls — works fully offline.
"""

from __future__ import annotations

import json
import re
import time
from collections import Counter, defaultdict
from dataclasses import dataclass, field, asdict
from difflib import SequenceMatcher
from pathlib import Path
from typing import Optional


# ── Storage ──────────────────────────────────────────────────────────────────

DATA_DIR = Path("ml_data")
HISTORY_PATH = DATA_DIR / "query_history.jsonl"
PATTERNS_PATH = DATA_DIR / "learned_patterns.json"
STATS_PATH = DATA_DIR / "model_stats.json"


@dataclass
class QueryExample:
    question: str
    sql: str
    schema: str
    dialect: str = "sqlite"
    timestamp: str = ""
    was_executed: bool = False
    row_count: int = 0


@dataclass
class MLResponse:
    sql: str
    model: str
    confidence: float = 0.0
    source: str = ""  # "pattern", "similarity", "template"
    similar_questions: list = field(default_factory=list)
    error: str | None = None


class LocalMLModel:
    """Self-learning SQL generation model."""

    def __init__(self):
        DATA_DIR.mkdir(exist_ok=True)
        self._history: list[QueryExample] = []
        self._patterns: dict = {}
        self._tfidf_cache: dict = {}
        self._idf: dict[str, float] = {}
        self._vocab: set[str] = set()
        self._load()

    # ── Persistence ──────────────────────────────────────────────────────────

    def _load(self):
        """Load history and patterns from disk."""
        if HISTORY_PATH.exists():
            for line in HISTORY_PATH.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    d = json.loads(line)
                    self._history.append(QueryExample(**d))
                except Exception:
                    pass

        if PATTERNS_PATH.exists():
            try:
                self._patterns = json.loads(
                    PATTERNS_PATH.read_text(encoding="utf-8")
                )
            except Exception:
                self._patterns = {}

        if self._history:
            self._rebuild_index()

    def _save_example(self, ex: QueryExample):
        """Append a single example to history."""
        self._history.append(ex)
        with open(HISTORY_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(asdict(ex), ensure_ascii=False) + "\n")
        self._rebuild_index()

    def _save_patterns(self):
        PATTERNS_PATH.write_text(
            json.dumps(self._patterns, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def _save_stats(self):
        stats = {
            "total_examples": len(self._history),
            "vocab_size": len(self._vocab),
            "patterns_count": len(self._patterns.get("intent_patterns", {})),
            "last_updated": time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        STATS_PATH.write_text(
            json.dumps(stats, ensure_ascii=False, indent=2), encoding="utf-8"
        )

    # ── Tokenization & TF-IDF ────────────────────────────────────────────────

    @staticmethod
    def _tokenize(text: str) -> list[str]:
        """Lowercase tokenization with basic normalization."""
        text = text.lower().strip()
        text = re.sub(r"[^\w\s]", " ", text)
        tokens = text.split()
        # Remove very short tokens
        return [t for t in tokens if len(t) > 1]

    def _rebuild_index(self):
        """Rebuild TF-IDF index from all history."""
        if not self._history:
            return

        # Build document frequency
        doc_count = len(self._history)
        df: Counter = Counter()
        for ex in self._history:
            tokens = set(self._tokenize(ex.question))
            for t in tokens:
                df[t] += 1
                self._vocab.add(t)

        import math

        self._idf = {
            t: math.log((doc_count + 1) / (freq + 1)) + 1
            for t, freq in df.items()
        }

        # Build TF-IDF vectors for all documents
        self._tfidf_cache = {}
        for i, ex in enumerate(self._history):
            self._tfidf_cache[i] = self._tfidf_vector(ex.question)

    def _tfidf_vector(self, text: str) -> dict[str, float]:
        """Compute TF-IDF vector for a text."""
        tokens = self._tokenize(text)
        if not tokens:
            return {}
        tf = Counter(tokens)
        max_tf = max(tf.values())
        vec = {}
        for t, count in tf.items():
            tf_norm = 0.5 + 0.5 * count / max_tf
            idf = self._idf.get(t, 1.0)
            vec[t] = tf_norm * idf
        return vec

    @staticmethod
    def _cosine_sim(a: dict[str, float], b: dict[str, float]) -> float:
        """Cosine similarity between two sparse vectors."""
        if not a or not b:
            return 0.0
        common = set(a.keys()) & set(b.keys())
        if not common:
            return 0.0
        dot = sum(a[k] * b[k] for k in common)
        norm_a = sum(v * v for v in a.values()) ** 0.5
        norm_b = sum(v * v for v in b.values()) ** 0.5
        if norm_a == 0 or norm_b == 0:
            return 0.0
        return dot / (norm_a * norm_b)

    # ── Intent detection ─────────────────────────────────────────────────────

    # Built-in SQL intent patterns (bootstrap — no training needed)
    _INTENT_MAP = {
        "count": {
            "keywords": [
                "how many", "count", "сколько", "количество", "число", "total number",
            ],
            "template": "SELECT COUNT(*) AS count FROM {table}",
        },
        "top_n": {
            "keywords": [
                "top", "largest", "biggest", "highest", "most", "best",
                "топ", "наибольш", "максимал", "лучш",
            ],
            "template": "SELECT * FROM {table} ORDER BY {column} DESC LIMIT {n}",
        },
        "bottom_n": {
            "keywords": [
                "bottom", "smallest", "lowest", "least", "worst", "minimum",
                "наименьш", "минимал", "худш",
            ],
            "template": "SELECT * FROM {table} ORDER BY {column} ASC LIMIT {n}",
        },
        "average": {
            "keywords": [
                "average", "avg", "mean", "среднее", "средн",
            ],
            "template": "SELECT AVG({column}) AS average FROM {table}",
        },
        "sum": {
            "keywords": [
                "sum", "total", "сумма", "итого", "всего",
            ],
            "template": "SELECT SUM({column}) AS total FROM {table}",
        },
        "group_by": {
            "keywords": [
                "by", "per", "each", "group", "breakdown", "distribution",
                "по", "для каждого", "разбивка", "распределение",
            ],
            "template": "SELECT {group_col}, COUNT(*) AS count FROM {table} GROUP BY {group_col} ORDER BY count DESC",
        },
        "filter": {
            "keywords": [
                "where", "filter", "only", "with", "that have",
                "где", "только", "фильтр", "которые",
            ],
            "template": "SELECT * FROM {table} WHERE {condition}",
        },
        "show_all": {
            "keywords": [
                "show", "list", "all", "display", "get",
                "показ", "список", "все", "вывести",
            ],
            "template": "SELECT * FROM {table} LIMIT 100",
        },
        "distinct": {
            "keywords": [
                "unique", "distinct", "different", "уникальн", "различн",
            ],
            "template": "SELECT DISTINCT {column} FROM {table}",
        },
        "join": {
            "keywords": [
                "join", "combine", "together", "related", "with",
                "объединить", "связать", "вместе",
            ],
            "template": "SELECT * FROM {table1} JOIN {table2} ON {table1}.{key} = {table2}.{key}",
        },
    }

    def _detect_intent(self, question: str) -> tuple[str, float]:
        """Detect SQL intent from question. Returns (intent, confidence)."""
        q_lower = question.lower()
        scores: dict[str, float] = {}

        for intent, info in self._INTENT_MAP.items():
            score = 0.0
            for kw in info["keywords"]:
                if kw in q_lower:
                    score += 1.0
            # Use learned patterns if available
            learned = self._patterns.get("intent_patterns", {}).get(intent, {})
            for kw, weight in learned.items():
                if kw in q_lower:
                    score += weight
            if score > 0:
                scores[intent] = score

        if not scores:
            return "show_all", 0.1

        best = max(scores, key=scores.get)
        # Normalize confidence
        conf = min(scores[best] / 3.0, 0.95)
        return best, conf

    # ── Schema parsing ───────────────────────────────────────────────────────

    @staticmethod
    def _parse_schema(schema: str) -> list[dict]:
        """Parse schema text into structured table info."""
        tables = []
        for line in schema.strip().splitlines():
            m = re.match(r"TABLE\s+(\w+):\s*(.+)", line, re.IGNORECASE)
            if not m:
                continue
            table_name = m.group(1)
            cols_raw = m.group(2)
            columns = []
            for col_m in re.finditer(r"(\w+)\s*\(([^)]+)\)", cols_raw):
                columns.append({
                    "name": col_m.group(1),
                    "type": col_m.group(2).strip(),
                })
            tables.append({"name": table_name, "columns": columns})
        return tables

    @staticmethod
    def _find_best_table(tables: list[dict], question: str) -> Optional[dict]:
        """Find the table most relevant to the question."""
        q_lower = question.lower()
        best_table = None
        best_score = 0.0

        for t in tables:
            score = 0.0
            tname = t["name"].lower()
            # Direct table name match
            if tname in q_lower or tname.rstrip("s") in q_lower:
                score += 3.0
            # Fuzzy match
            score += SequenceMatcher(None, tname, q_lower).ratio()
            # Column name matches
            for col in t["columns"]:
                cname = col["name"].lower()
                if cname in q_lower:
                    score += 1.5
            if score > best_score:
                best_score = score
                best_table = t

        return best_table

    @staticmethod
    def _find_best_column(
        columns: list[dict], question: str, prefer_numeric: bool = False
    ) -> Optional[str]:
        """Find the column most relevant to the question."""
        q_lower = question.lower()
        best_col = None
        best_score = 0.0

        numeric_types = {"integer", "int", "float", "real", "numeric", "decimal", "bigint", "smallint", "double"}

        for col in columns:
            cname = col["name"].lower()
            ctype = col["type"].lower()
            score = 0.0

            if cname in q_lower:
                score += 3.0
            score += SequenceMatcher(None, cname, q_lower).ratio() * 0.5

            if prefer_numeric:
                if any(nt in ctype for nt in numeric_types):
                    score += 1.0
                else:
                    score -= 0.5

            # Skip id columns for aggregation
            if prefer_numeric and cname in ("id", "rowid", "pk"):
                score -= 2.0

            if score > best_score:
                best_score = score
                best_col = col["name"]

        return best_col

    @staticmethod
    def _extract_number(question: str) -> int:
        """Extract a number from question (for top N queries)."""
        m = re.search(r"\b(\d{1,4})\b", question)
        return int(m.group(1)) if m else 10

    # ── SQL generation ───────────────────────────────────────────────────────

    def generate(
        self, question: str, schema: str, dialect: str = "sqlite"
    ) -> MLResponse:
        """Generate SQL from natural language using local ML."""
        if not question.strip():
            return MLResponse(sql="", model="local-ml", error="Empty question")

        tables = self._parse_schema(schema)
        if not tables:
            return MLResponse(
                sql="", model="local-ml", error="No tables found in schema"
            )

        # Strategy 1: Find similar past queries (highest confidence)
        if self._history:
            sim_result = self._find_similar(question, schema, dialect)
            if sim_result and sim_result.confidence >= 0.65:
                return sim_result

        # Strategy 2: Intent + schema matching (template-based)
        intent, intent_conf = self._detect_intent(question)
        table = self._find_best_table(tables, question)

        if table:
            sql = self._build_from_intent(intent, table, tables, question, dialect)
            if sql:
                # Combine confidences
                confidence = min(intent_conf * 0.8 + 0.15, 0.90)
                similar = []
                if self._history:
                    sim_result = self._find_similar(question, schema, dialect)
                    if sim_result and sim_result.similar_questions:
                        similar = sim_result.similar_questions

                return MLResponse(
                    sql=sql,
                    model="local-ml",
                    confidence=round(confidence, 2),
                    source="template",
                    similar_questions=similar,
                )

        # Strategy 3: Best effort — simple SELECT
        fallback_table = tables[0]["name"]
        return MLResponse(
            sql=f"SELECT * FROM {fallback_table} LIMIT 100",
            model="local-ml",
            confidence=0.15,
            source="fallback",
        )

    def _find_similar(
        self, question: str, schema: str, dialect: str
    ) -> Optional[MLResponse]:
        """Find the most similar past query and adapt it."""
        if not self._history:
            return None

        q_vec = self._tfidf_vector(question)
        if not q_vec:
            return None

        scores = []
        for i, ex in enumerate(self._history):
            vec = self._tfidf_cache.get(i)
            if not vec:
                continue
            sim = self._cosine_sim(q_vec, vec)
            if sim > 0.3:
                scores.append((sim, i, ex))

        if not scores:
            return None

        scores.sort(key=lambda x: x[0], reverse=True)
        top = scores[:3]

        best_sim, best_idx, best_ex = top[0]

        # Adapt SQL to current schema
        adapted_sql = self._adapt_sql(best_ex.sql, best_ex.schema, schema)

        similar_qs = [
            {"question": ex.question, "similarity": round(s, 2)}
            for s, _, ex in top
        ]

        return MLResponse(
            sql=adapted_sql,
            model="local-ml",
            confidence=round(min(best_sim * 0.95, 0.95), 2),
            source="similarity",
            similar_questions=similar_qs,
        )

    def _adapt_sql(self, sql: str, old_schema: str, new_schema: str) -> str:
        """Adapt SQL from old schema to new schema (table/column renaming)."""
        if old_schema == new_schema:
            return sql

        old_tables = self._parse_schema(old_schema)
        new_tables = self._parse_schema(new_schema)

        if not old_tables or not new_tables:
            return sql

        # Build mapping of old table names → new table names
        table_map = {}
        for ot in old_tables:
            best_match = None
            best_sim = 0.0
            for nt in new_tables:
                sim = SequenceMatcher(
                    None, ot["name"].lower(), nt["name"].lower()
                ).ratio()
                if sim > best_sim:
                    best_sim = sim
                    best_match = nt["name"]
            if best_match and best_sim > 0.5:
                table_map[ot["name"]] = best_match

        adapted = sql
        for old_name, new_name in table_map.items():
            if old_name != new_name:
                adapted = re.sub(
                    rf"\b{re.escape(old_name)}\b", new_name, adapted
                )

        return adapted

    def _build_from_intent(
        self,
        intent: str,
        table: dict,
        all_tables: list[dict],
        question: str,
        dialect: str,
    ) -> Optional[str]:
        """Build SQL from detected intent and schema."""
        t_name = table["name"]
        columns = table["columns"]

        if intent == "count":
            return f"SELECT COUNT(*) AS count FROM {t_name}"

        if intent == "show_all":
            return f"SELECT * FROM {t_name} LIMIT 100"

        if intent == "top_n":
            n = self._extract_number(question)
            col = self._find_best_column(columns, question, prefer_numeric=True)
            if col:
                return f"SELECT * FROM {t_name} ORDER BY {col} DESC LIMIT {n}"
            return f"SELECT * FROM {t_name} LIMIT {n}"

        if intent == "bottom_n":
            n = self._extract_number(question)
            col = self._find_best_column(columns, question, prefer_numeric=True)
            if col:
                return f"SELECT * FROM {t_name} ORDER BY {col} ASC LIMIT {n}"
            return f"SELECT * FROM {t_name} LIMIT {n}"

        if intent == "average":
            col = self._find_best_column(columns, question, prefer_numeric=True)
            if col:
                return f"SELECT AVG({col}) AS average_{col} FROM {t_name}"
            return None

        if intent == "sum":
            col = self._find_best_column(columns, question, prefer_numeric=True)
            if col:
                return f"SELECT SUM({col}) AS total_{col} FROM {t_name}"
            return None

        if intent == "distinct":
            col = self._find_best_column(columns, question, prefer_numeric=False)
            if col:
                return f"SELECT DISTINCT {col} FROM {t_name} ORDER BY {col}"
            return None

        if intent == "group_by":
            # Find a grouping column (text/varchar) and a numeric column
            group_col = self._find_best_column(columns, question, prefer_numeric=False)
            agg_col = self._find_best_column(columns, question, prefer_numeric=True)
            if group_col and agg_col and group_col != agg_col:
                return (
                    f"SELECT {group_col}, SUM({agg_col}) AS total, COUNT(*) AS count "
                    f"FROM {t_name} GROUP BY {group_col} ORDER BY total DESC"
                )
            if group_col:
                return (
                    f"SELECT {group_col}, COUNT(*) AS count "
                    f"FROM {t_name} GROUP BY {group_col} ORDER BY count DESC"
                )
            return None

        if intent == "filter":
            col = self._find_best_column(columns, question, prefer_numeric=False)
            if col:
                # Try to extract value from question
                return f"SELECT * FROM {t_name} WHERE {col} IS NOT NULL LIMIT 100"
            return None

        if intent == "join":
            if len(all_tables) >= 2:
                t2 = None
                for t in all_tables:
                    if t["name"] != t_name:
                        t2 = t
                        break
                if t2:
                    # Find common column (FK heuristic)
                    t1_cols = {c["name"].lower() for c in columns}
                    t2_cols = {c["name"].lower() for c in t2["columns"]}
                    common = t1_cols & t2_cols
                    # Also check for table_name_id pattern
                    fk_pattern = t_name.lower().rstrip("s") + "_id"
                    fk2_pattern = t2["name"].lower().rstrip("s") + "_id"

                    join_col = None
                    if common - {"id"}:
                        join_col = (common - {"id"}).pop()
                    elif fk_pattern in t2_cols:
                        join_col = fk_pattern
                    elif fk2_pattern in t1_cols:
                        join_col = fk2_pattern
                    elif "id" in common:
                        join_col = "id"

                    if join_col:
                        return (
                            f"SELECT * FROM {t_name} "
                            f"JOIN {t2['name']} ON {t_name}.{join_col} = {t2['name']}.{join_col} "
                            f"LIMIT 100"
                        )

        return None

    # ── Learning ─────────────────────────────────────────────────────────────

    def learn(
        self,
        question: str,
        sql: str,
        schema: str,
        dialect: str = "sqlite",
        was_executed: bool = False,
        row_count: int = 0,
    ):
        """Learn from a successful query pair."""
        if not question.strip() or not sql.strip():
            return

        # Don't store duplicates
        for ex in self._history[-50:]:
            if ex.question.strip().lower() == question.strip().lower():
                return

        ex = QueryExample(
            question=question,
            sql=sql,
            schema=schema,
            dialect=dialect,
            timestamp=time.strftime("%Y-%m-%d %H:%M:%S"),
            was_executed=was_executed,
            row_count=row_count,
        )
        self._save_example(ex)

        # Update intent patterns from this example
        self._learn_intent_patterns(question, sql)
        self._save_patterns()
        self._save_stats()

    def _learn_intent_patterns(self, question: str, sql: str):
        """Extract and reinforce intent patterns from a question-SQL pair."""
        if "intent_patterns" not in self._patterns:
            self._patterns["intent_patterns"] = {}

        sql_upper = sql.upper().strip()
        tokens = self._tokenize(question)

        # Detect which intent this SQL corresponds to
        detected_intents = []
        if "COUNT(" in sql_upper:
            detected_intents.append("count")
        if "AVG(" in sql_upper:
            detected_intents.append("average")
        if "SUM(" in sql_upper:
            detected_intents.append("sum")
        if "GROUP BY" in sql_upper:
            detected_intents.append("group_by")
        if "ORDER BY" in sql_upper and "DESC" in sql_upper:
            if "LIMIT" in sql_upper:
                detected_intents.append("top_n")
        if "ORDER BY" in sql_upper and "ASC" in sql_upper:
            if "LIMIT" in sql_upper:
                detected_intents.append("bottom_n")
        if "DISTINCT" in sql_upper:
            detected_intents.append("distinct")
        if "JOIN" in sql_upper:
            detected_intents.append("join")
        if "WHERE" in sql_upper:
            detected_intents.append("filter")

        # Reinforce: associate question tokens with detected intents
        for intent in detected_intents:
            if intent not in self._patterns["intent_patterns"]:
                self._patterns["intent_patterns"][intent] = {}
            for token in tokens:
                if len(token) > 2:
                    current = self._patterns["intent_patterns"][intent].get(
                        token, 0.0
                    )
                    # Exponential moving average
                    self._patterns["intent_patterns"][intent][token] = (
                        current * 0.8 + 0.2
                    )

    # ── Stats ────────────────────────────────────────────────────────────────

    def get_stats(self) -> dict:
        """Return model statistics."""
        return {
            "total_examples": len(self._history),
            "vocab_size": len(self._vocab),
            "patterns_count": len(self._patterns.get("intent_patterns", {})),
            "intent_distribution": self._intent_distribution(),
            "model": "local-ml (TF-IDF + intent templates)",
        }

    def _intent_distribution(self) -> dict[str, int]:
        """Count examples per detected intent."""
        dist: Counter = Counter()
        for ex in self._history:
            sql_upper = ex.sql.upper()
            if "COUNT(" in sql_upper:
                dist["count"] += 1
            elif "AVG(" in sql_upper:
                dist["average"] += 1
            elif "SUM(" in sql_upper:
                dist["sum"] += 1
            elif "GROUP BY" in sql_upper:
                dist["group_by"] += 1
            elif "JOIN" in sql_upper:
                dist["join"] += 1
            elif "DISTINCT" in sql_upper:
                dist["distinct"] += 1
            else:
                dist["select"] += 1
        return dict(dist)


# Singleton
_model: LocalMLModel | None = None


def get_model() -> LocalMLModel:
    global _model
    if _model is None:
        _model = LocalMLModel()
    return _model
