"""Bilingual (EN + RU) synonym expansion and column-semantics hints.

Static and explicit on purpose - no model. Used to make lexical retrieval
generalize and to map words like 'profitable'/'dokhod' onto numeric columns."""
from __future__ import annotations

# canonical to surface variants (lowercased, ASCII transliteration for RU)
_GROUPS = {
    "count": ["count", "number", "how many", "skolko", "kolichestvo", "chislo"],
    "average": ["average", "avg", "mean", "srednee", "sredn", "sredniy"],
    "sum": ["sum", "total", "summa", "itogo", "vsego"],
    "top": ["top", "best", "highest", "most", "largest", "luchshie", "top", "naibol"],
    "money": ["profit", "profitable", "revenue", "income", "earnings", "pnl",
              "dokhod", "dokhodnye", "pribyl", "vyruchka", "money"],
    "time": ["date", "time", "when", "day", "month", "year", "data", "vremya"],
    "unique": ["unique", "distinct", "different", "unikaln", "razlichn"],
}

# word to column semantic tag
_HINT = {}
for _tag, _key in (("money", "money"), ("avg", "average"),
                   ("count", "count"), ("time", "time")):
    for _w in _GROUPS[_key]:
        _HINT[_w] = _tag


def expand(tokens: list) -> list:
    """Return tokens plus the canonical group key for any token that belongs
    to a synonym group (deduped, originals first)."""
    out = list(tokens)
    seen = set(tokens)
    for tok in tokens:
        for key, variants in _GROUPS.items():
            if tok in variants and key not in seen:
                out.append(key)
                seen.add(key)
    return out


def column_hint(word: str) -> str | None:
    """Semantic tag for a single word, or None."""
    w = (word or "").lower()
    if w in _HINT:
        return _HINT[w]
    for full, tag in _HINT.items():
        if full in w or w in full:
            return tag
    return None
