"""sqlglot-backed validation, dialect transpile, and schema adaptation.

Every function degrades to a no-op (returns its input, ok=True) when sqlglot is
missing or cannot parse, so the generator never breaks."""
from __future__ import annotations

from difflib import SequenceMatcher

from querypad.ml.schema import Schema

try:
    import sqlglot
    from sqlglot import exp
except Exception:  # pragma: no cover - import guard
    sqlglot = None
    exp = None

_DIALECT = {"sqlite": "sqlite", "postgresql": "postgres", "postgres": "postgres",
            "mysql": "mysql", "clickhouse": "clickhouse", "mssql": "tsql",
            "sqlserver": "tsql"}


def validate(sql: str, schema_text: str, dialect: str = "sqlite"):
    if not sqlglot or not sql.strip():
        return (sql, True, "")
    try:
        tree = sqlglot.parse_one(sql, read="sqlite")
    except Exception as exc:
        return (sql, True, f"unparsed: {exc}")

    schema = Schema.parse(schema_text)
    known_tables = {t.name.lower() for t in schema.tables}
    known_cols = {c.name.lower() for t in schema.tables for c in t.columns}

    for tbl in tree.find_all(exp.Table):
        if tbl.name and tbl.name.lower() not in known_tables:
            return (sql, False, f"unknown table: {tbl.name}")
    for col in tree.find_all(exp.Column):
        if col.name and col.name.lower() not in known_cols:
            return (sql, False, f"unknown column: {col.name}")

    out_dialect = _DIALECT.get(dialect, "sqlite")
    try:
        out = tree.sql(dialect=out_dialect)
    except Exception:
        out = sql
    return (out, True, "")


def adapt(sql: str, old_schema_text: str, new_schema_text: str) -> str:
    if not sqlglot or old_schema_text == new_schema_text:
        return sql
    old, new = Schema.parse(old_schema_text), Schema.parse(new_schema_text)
    if not old.tables or not new.tables:
        return sql

    def best(name, candidates):
        bn, bs = None, 0.0
        for cand in candidates:
            r = SequenceMatcher(None, name.lower(), cand.lower()).ratio()
            if r > bs:
                bn, bs = cand, r
        return bn if bs > 0.5 else None

    if len(old.tables) == 1 and len(new.tables) == 1:
        tmap = {old.tables[0].name: new.tables[0].name}
    else:
        tmap = {t.name: best(t.name, [x.name for x in new.tables]) for t in old.tables}
    new_cols = {c.name for t in new.tables for c in t.columns}
    old_cols = {c.name for t in old.tables for c in t.columns}
    cmap = {}
    for oc in old_cols:
        if oc not in new_cols:
            if len(old_cols - new_cols) == 1 and len(new_cols - old_cols) == 1:
                cand = next(iter(new_cols - old_cols))
            else:
                cand = best(oc, list(new_cols))
            if cand:
                cmap[oc] = cand
    try:
        tree = sqlglot.parse_one(sql, read="sqlite")
        for tbl in tree.find_all(exp.Table):
            if tbl.name in tmap and tmap[tbl.name]:
                tbl.set("this", exp.to_identifier(tmap[tbl.name]))
        for col in tree.find_all(exp.Column):
            if col.name in cmap:
                col.set("this", exp.to_identifier(cmap[col.name]))
        return tree.sql(dialect="sqlite")
    except Exception:
        return sql
