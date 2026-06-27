"""Local-ML orchestrator: retrieval-first, then the slot-filling pipeline."""
from __future__ import annotations

import json
import math
import re
import time
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path

from querypad.ml.builder import build
from querypad.ml.extract import extract_slots
from querypad.ml.intent import IntentClassifier, intent_of_sql
from querypad.ml.schema import Schema
from querypad.ml.synonyms import expand
from querypad.ml.validate import adapt, validate

DATA_DIR = Path("ml_data")
HISTORY_PATH = DATA_DIR / "query_history.jsonl"
INTENT_PATH = DATA_DIR / "intent_model.json"
STATS_PATH = DATA_DIR / "model_stats.json"


@dataclass
class MLResponse:
    sql: str
    model: str
    confidence: float = 0.0
    source: str = ""
    similar_questions: list = field(default_factory=list)
    error: str | None = None


def _tokenize(text: str) -> list:
    text = re.sub(r"[^\w\s]", " ", (text or "").lower())
    return expand([t for t in text.split() if len(t) > 1])


def _success_weight(ex: dict) -> float:
    if ex.get("corrected"):
        return 1.3
    if ex.get("was_executed") and ex.get("row_count", 0) > 0:
        return 1.0
    if ex.get("was_executed"):
        return 0.5
    return 0.3


class LocalMLModel:
    def __init__(self):
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        self._history: list = []
        self._idf: dict = {}
        self._vecs: list = []
        self._clf = IntentClassifier(path=INTENT_PATH)
        self._load()

    def _load(self):
        if HISTORY_PATH.exists():
            for line in HISTORY_PATH.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    self._history.append(json.loads(line))
                except Exception:
                    pass
        self._reindex()

    def _reindex(self):
        if not self._history:
            self._idf, self._vecs = {}, []
            return
        n = len(self._history)
        df = Counter()
        for ex in self._history:
            for t in set(_tokenize(ex["question"])):
                df[t] += 1
        self._idf = {t: math.log((n + 1) / (f + 1)) + 1 for t, f in df.items()}
        self._vecs = [self._vec(ex["question"]) for ex in self._history]

    def _vec(self, text: str) -> dict:
        toks = _tokenize(text)
        if not toks:
            return {}
        tf = Counter(toks)
        mx = max(tf.values())
        return {t: (0.5 + 0.5 * c / mx) * self._idf.get(t, 1.0) for t, c in tf.items()}

    @staticmethod
    def _cos(a: dict, b: dict) -> float:
        common = set(a) & set(b)
        if not common:
            return 0.0
        dot = sum(a[k] * b[k] for k in common)
        na = math.sqrt(sum(v * v for v in a.values()))
        nb = math.sqrt(sum(v * v for v in b.values()))
        return dot / (na * nb) if na and nb else 0.0

    @staticmethod
    def _resolve_table(question: str, sch: Schema):
        """Schema.find_table() is a lexical/fuzzy match against the whole
        question; with only one table that has no literal-token overlap with
        the question (e.g. a transliterated-Russian word with no synonym
        entry), its whole-string ratio is noise and can pick the wrong table
        among several. When that happens, prefer the table whose columns
        actually match a synonym-expanded semantic hint (e.g. "money" maps
        onto a table with a real numeric pnl/stake column) over one with no
        such column at all."""
        table = sch.find_table(question)
        if table is None or len(sch.tables) < 2:
            return table

        toks = _tokenize(question)
        names = {table.name.lower()} | {c.name.lower() for c in table.columns}
        literal_hit = any(t in names or any(t in n for n in names) for t in toks)
        if literal_hit:
            return table

        hints = {expand([t])[-1] for t in toks if expand([t]) != [t]}
        if "money" not in hints:
            return table

        def has_money_col(t):
            return any(c.is_numeric and c.name.lower() not in ("id", "rowid", "pk")
                       and not c.name.lower().endswith("_id")
                       for c in t.columns)

        if not has_money_col(table):
            better = next((t for t in sch.tables if has_money_col(t)), None)
            if better:
                return better
        return table

    def generate(self, question: str, schema: str, dialect: str = "sqlite") -> MLResponse:
        if not question.strip():
            return MLResponse(sql="", model="local-ml", error="Empty question")
        sch = Schema.parse(schema)
        if not sch.tables:
            return MLResponse(sql="", model="local-ml", error="No tables found in schema")

        sim = self._retrieve(question, schema, dialect)
        if sim and sim.confidence >= 0.65:
            return sim

        intent, prob = self._clf.predict(question)
        table = self._resolve_table(question, sch)
        if table:
            slots = extract_slots(question, sch, table)
            # extract_slots runs its own explicit top/bottom-phrase regex
            # (independent of the NB classifier) and is a stronger signal
            # than a low-confidence intent guess: if it found an explicit
            # sort direction but the classifier did not predict a ranking
            # intent, trust the slot extractor.
            if slots.order_by and intent not in ("top_n", "bottom_n"):
                intent = "top_n" if slots.order_by[1] == "DESC" else "bottom_n"
            # A weak NB guess of an aggregation intent, with no aggregation word
            # in the question, means the user wants to SEE rows (optionally
            # filtered), not aggregate. Trust the slots over the guess.
            _agg_words = ("count", "how many", "average", "avg", "mean", "sum",
                          "total", "group", "distinct", "unique", "skolko",
                          "srednee", "summa", "gruppa", "kolichestvo")
            if intent in ("average", "sum", "count", "group_by", "distinct") \
                    and not any(w in question.lower() for w in _agg_words):
                intent = "filter" if slots.filters else "show_all"
            sql = build(intent, table, sch, slots, dialect)
            if sql:
                sql, ok, _note = validate(sql, schema, dialect)
                conf = min(prob * 0.7 + (0.2 if slots.filters or slots.order_by else 0.1), 0.9)
                conf = conf if ok else conf * 0.6
                return MLResponse(sql=sql, model="local-ml", confidence=round(conf, 2),
                                  source="template",
                                  similar_questions=(sim.similar_questions if sim else []))

        return MLResponse(sql=f"SELECT * FROM {sch.tables[0].name} LIMIT 100",
                          model="local-ml", confidence=0.15, source="fallback")

    def _retrieve(self, question, schema, dialect):
        if not self._history:
            return None
        qv = self._vec(question)
        if not qv:
            return None
        scored = []
        for i, ex in enumerate(self._history):
            base = self._cos(qv, self._vecs[i])
            if base > 0.3:
                scored.append((base * _success_weight(ex), base, ex))
        if not scored:
            return None
        scored.sort(key=lambda x: x[0], reverse=True)
        top = scored[:3]
        _w, best_sim, best = top[0]
        sql = adapt(best["sql"], best.get("schema", ""), schema)
        sql, _ok, _note = validate(sql, schema, dialect)
        similar = [{"question": e["question"], "similarity": round(s, 2)} for _w, s, e in top]
        return MLResponse(sql=sql, model="local-ml",
                          confidence=round(min(best_sim * 0.95, 0.95), 2),
                          source="similarity", similar_questions=similar)

    def learn(self, question, sql, schema, dialect="sqlite", was_executed=False,
              row_count=0, ai_sql=None):
        if not question.strip() or not sql.strip():
            return
        for ex in self._history[-50:]:
            if ex["question"].strip().lower() == question.strip().lower():
                return
        ex = {"question": question, "sql": sql, "schema": schema, "dialect": dialect,
              "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
              "was_executed": was_executed, "row_count": row_count,
              "corrected": bool(ai_sql and ai_sql.strip() != sql.strip())}
        self._history.append(ex)
        with open(HISTORY_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(ex, ensure_ascii=False) + "\n")
        self._reindex()
        intent = intent_of_sql(sql)
        if intent:
            self._clf.partial_fit(question, intent)
        self._save_stats()

    def _save_stats(self):
        STATS_PATH.write_text(json.dumps({
            "total_examples": len(self._history),
            "last_updated": time.strftime("%Y-%m-%d %H:%M:%S"),
        }, ensure_ascii=True), encoding="utf-8")

    def get_stats(self) -> dict:
        dist = Counter(intent_of_sql(ex["sql"]) for ex in self._history)
        return {"total_examples": len(self._history),
                "intent_distribution": dict(dist),
                "model": "local-ml (NB intent + slots + retrieval)"}


_model = None


def get_model() -> LocalMLModel:
    global _model
    if _model is None:
        _model = LocalMLModel()
    return _model
