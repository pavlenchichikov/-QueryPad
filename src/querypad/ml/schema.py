"""Typed schema model parsed from the 'TABLE name: col (TYPE), ...' text."""
from __future__ import annotations

import re
from dataclasses import dataclass
from difflib import SequenceMatcher

_NUMERIC = {"int", "integer", "float", "real", "numeric", "decimal",
            "bigint", "smallint", "double"}
_DATE = {"date", "datetime", "timestamp", "time"}
_SKIP_AGG = {"id", "rowid", "pk"}


@dataclass
class Column:
    name: str
    type: str

    @property
    def is_numeric(self) -> bool:
        t = self.type.lower()
        return any(n in t for n in _NUMERIC)

    @property
    def is_date(self) -> bool:
        t = self.type.lower()
        n = self.name.lower()
        return any(d in t for d in _DATE) or any(d in n for d in _DATE)

    @property
    def is_text(self) -> bool:
        return not self.is_numeric and not self.is_date


@dataclass
class Table:
    name: str
    columns: list

    def column(self, name: str):
        for c in self.columns:
            if c.name.lower() == name.lower():
                return c
        return None


class Schema:
    def __init__(self, tables: list):
        self.tables = tables

    @classmethod
    def parse(cls, text: str) -> Schema:
        tables = []
        for line in (text or "").strip().splitlines():
            m = re.match(r"TABLE\s+(\w+):\s*(.+)", line, re.IGNORECASE)
            if not m:
                continue
            cols = [Column(cm.group(1), cm.group(2).strip())
                    for cm in re.finditer(r"(\w+)\s*\(([^)]+)\)", m.group(2))]
            tables.append(Table(m.group(1), cols))
        return cls(tables)

    def find_table(self, question: str):
        q = (question or "").lower()
        best, best_score = None, 0.0
        for t in self.tables:
            tn = t.name.lower()
            score = 0.0
            if tn in q or tn.rstrip("s") in q:
                score += 3.0
            score += SequenceMatcher(None, tn, q).ratio()
            for c in t.columns:
                if c.name.lower() in q:
                    score += 1.5
            if score > best_score:
                best, best_score = t, score
        return best

    def find_column(self, table: Table, question: str,
                    prefer_numeric: bool = False, prefer_text: bool = False):
        try:
            from querypad.ml.synonyms import column_hint
        except Exception:
            def column_hint(_w):
                return None
        q = (question or "").lower()
        hint = None
        for w in re.findall(r"\w+", q):
            hint = column_hint(w) or hint
        best, best_score = None, 0.0
        for c in (table.columns if table else []):
            cn = c.name.lower()
            score = SequenceMatcher(None, cn, q).ratio() * 0.5
            if cn in q:
                score += 3.0
            if prefer_numeric:
                score += 1.0 if c.is_numeric else -0.5
                if cn in _SKIP_AGG:
                    score -= 2.0
            if prefer_text and c.is_text:
                score += 1.0
            if hint == "money" and c.is_numeric and cn not in _SKIP_AGG:
                score += 1.0
            if score > best_score:
                best, best_score = c, score
        return best

    def join_keys(self, t1: Table, t2: Table):
        if not t1 or not t2:
            return None
        c1 = {c.name.lower() for c in t1.columns}
        c2 = {c.name.lower() for c in t2.columns}
        shared = (c1 & c2) - {"id"}
        if shared:
            k = sorted(shared)[0]
            return (k, k)
        fk1 = t1.name.lower().rstrip("s") + "_id"
        fk2 = t2.name.lower().rstrip("s") + "_id"
        if fk1 in c2:
            return ("id", fk1)
        if fk2 in c1:
            return (fk2, "id")
        if "id" in (c1 & c2):
            return ("id", "id")
        return None

    def has(self, table: str, col: str | None = None) -> bool:
        t = next((x for x in self.tables if x.name.lower() == table.lower()), None)
        if t is None:
            return False
        return col is None or t.column(col) is not None
