"""Slot extraction: turn a question into (target columns, filters, order, limit)
so the builder can emit real WHERE / ORDER BY. Bilingual (EN + RU)."""
from __future__ import annotations

import re
from dataclasses import dataclass, field

from querypad.ml.synonyms import column_hint

# phrase -> SQL operator
_OPS = [
    (r"\b(more than|greater than|over|above|bolshe|svyshe|bolee)\b", ">"),
    (r"\b(less than|under|below|fewer than|menshe|menee)\b", "<"),
    (r"\b(at least|>=|ne menee)\b", ">="),
    (r"\b(at most|<=|ne bolee)\b", "<="),
    (r"\b(before|do|ranshe)\b", "<"),
    (r"\b(after|posle|pozzhe)\b", ">"),
    (r"\b(contains|like|soderzh)\b", "LIKE"),
]
_TOP = r"\b(top|highest|most|largest|best|samye|luchshie|naibol)\b"
_BOTTOM = r"\b(bottom|lowest|least|smallest|worst|naimen|khudsh)\b"


@dataclass
class Slots:
    target_columns: list = field(default_factory=list)
    filters: list = field(default_factory=list)
    order_by: tuple | None = None
    limit: int | None = None


def _resolve_column(word: str, table) -> str | None:
    """Best column for a single word: exact, then money-hint, then None."""
    w = word.lower()
    for c in table.columns:
        if c.name.lower() == w or c.name.lower().rstrip("s") == w.rstrip("s"):
            return c.name
    if column_hint(w) == "money":
        for c in table.columns:
            if c.is_numeric and c.name.lower() not in ("id", "rowid", "pk"):
                return c.name
    return None


def extract_slots(question: str, schema, table) -> Slots:
    slots = Slots()
    if table is None:
        return slots
    q = question or ""
    ql = q.lower()
    words = re.findall(r"\w+", ql)

    # limit / order direction
    m = re.search(r"\b(\d{1,4})\b", ql)
    if re.search(_TOP, ql) or re.search(_BOTTOM, ql):
        slots.limit = int(m.group(1)) if m else 10
    direction = "DESC" if re.search(_TOP, ql) else ("ASC" if re.search(_BOTTOM, ql) else None)

    # order column: an explicit "by <col>" or a money word
    order_col = None
    mby = re.search(r"\bby\s+(\w+)", ql)
    if mby:
        order_col = _resolve_column(mby.group(1), table)
    if not order_col:
        for w in words:
            if column_hint(w) == "money":
                order_col = _resolve_column(w, table)
                if order_col:
                    break
    if order_col and direction is None and (
        re.search(_TOP, ql) or "dokhod" in ql or "profit" in ql
    ):
        direction = "DESC"
    if order_col and direction:
        slots.order_by = (order_col, direction)

    # numeric comparison filters: "<col> <op-phrase> <number>"
    for pat, op in _OPS:
        if op in ("LIKE",):
            continue
        for fm in re.finditer(pat, ql):
            num = re.search(r"(\d+(?:\.\d+)?)", ql[fm.end():fm.end() + 20])
            if not num:
                continue
            col = None
            for c in table.columns:
                if c.is_numeric and c.name.lower() in ql[:fm.start()]:
                    col = c.name
            if col is None:
                for c in table.columns:
                    if c.is_numeric and c.name.lower() not in ("id", "rowid", "pk"):
                        col = c.name
                        break
            if col:
                slots.filters.append((col, op, num.group(1)))

    # entity equality / LIKE: "<text-col> is/= <Value>" or capitalized entity
    for c in table.columns:
        if not c.is_text:
            continue
        cn = c.name.lower()
        m2 = re.search(rf"\b{re.escape(cn)}\b\s*(?:is|=|equals|sport)?\s*([A-Z][\w]+)", q)
        if m2:
            slots.filters.append((c.name, "=", f"'{m2.group(1)}'"))
    # capitalized entities distributed across team-like text columns, so a
    # "X vs Y" question maps X to the first team column and Y to the second
    # (rather than both landing on home_team and never matching)
    team_cols = [c for c in table.columns if c.is_text and
                 ("team" in c.name.lower() or "name" in c.name.lower())]
    if team_cols:
        ents = [e for e in re.findall(r"\b([A-Z][a-z]+)\b", q)
                if e.lower() not in ("select", "from", "table")]
        for i, ent in enumerate(ents):
            col = team_cols[i % len(team_cols)]
            if not any(x[2].strip("%'") == ent for x in slots.filters):
                slots.filters.append((col.name, "LIKE", f"'%{ent}%'"))

    return slots
