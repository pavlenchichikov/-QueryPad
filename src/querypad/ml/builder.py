"""Compose SQL from an intent, a table, and extracted slots."""
from __future__ import annotations


def _where(slots) -> str:
    if not slots.filters:
        return ""
    parts = []
    for col, op, val in slots.filters:
        parts.append(f"{col} {op} {val}")
    return " WHERE " + " AND ".join(parts)


def _order_limit(slots, default_limit=None) -> str:
    out = ""
    if slots.order_by:
        out += f" ORDER BY {slots.order_by[0]} {slots.order_by[1]}"
    lim = slots.limit or default_limit
    if lim:
        out += f" LIMIT {lim:d}"
    return out


def _numeric_col(table, exclude=("id", "rowid", "pk")):
    for c in table.columns:
        if c.is_numeric and c.name.lower() not in exclude:
            return c.name
    return None


def _text_col(table):
    for c in table.columns:
        if c.is_text:
            return c.name
    return None


def build(intent, table, schema, slots, dialect: str = "sqlite") -> str | None:
    if table is None:
        return None
    t = table.name

    if intent == "count":
        return f"SELECT COUNT(*) AS count FROM {t}{_where(slots)}"

    if intent == "show_all":
        return f"SELECT * FROM {t}{_where(slots)}{_order_limit(slots, 100)}"

    if intent in ("top_n", "bottom_n"):
        if not slots.order_by:
            col = _numeric_col(table)
            if col:
                slots.order_by = (col, "DESC" if intent == "top_n" else "ASC")
        return (f"SELECT * FROM {t}{_where(slots)}"
                f"{_order_limit(slots, slots.limit or 10)}")

    if intent in ("average", "sum"):
        col = (slots.target_columns[0] if slots.target_columns else None) or _numeric_col(table)
        if not col:
            return None
        fn = "AVG" if intent == "average" else "SUM"
        alias = ("average_" if intent == "average" else "total_") + col
        return f"SELECT {fn}({col}) AS {alias} FROM {t}{_where(slots)}"

    if intent == "distinct":
        col = (slots.target_columns[0] if slots.target_columns else None) or _text_col(table)
        if not col:
            return None
        return f"SELECT DISTINCT {col} FROM {t}{_where(slots)} ORDER BY {col}"

    if intent == "group_by":
        group = (slots.target_columns[0] if slots.target_columns else None) or _text_col(table)
        if not group:
            return None
        agg = _numeric_col(table)
        if agg and agg != group:
            return (f"SELECT {group}, SUM({agg}) AS total, COUNT(*) AS count FROM {t}"
                    f"{_where(slots)} GROUP BY {group} ORDER BY total DESC")
        return (f"SELECT {group}, COUNT(*) AS count FROM {t}{_where(slots)} "
                f"GROUP BY {group} ORDER BY count DESC")

    if intent == "filter":
        return (f"SELECT * FROM {t}{_where(slots) or ' WHERE 1=1'}"
                f"{_order_limit(slots, 100)}")

    if intent == "join":
        others = [x for x in schema.tables if x.name != t]
        if others:
            t2 = others[0]
            keys = schema.join_keys(table, t2)
            if keys:
                return (f"SELECT * FROM {t} JOIN {t2.name} ON "
                        f"{t}.{keys[0]} = {t2.name}.{keys[1]}{_where(slots)} LIMIT 100")
    return None
