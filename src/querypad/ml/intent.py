"""Multinomial Naive-Bayes intent classifier (pure standard library).

Learns intent from (question, intent) pairs; bootstrapped with pseudo-counts
from a keyword seed so it predicts sensibly with zero history. Replaces the old
keyword-OR detector and its pattern-pollution learning."""
from __future__ import annotations

import json
import math
import re
from pathlib import Path

from querypad.ml.synonyms import expand

INTENTS = ["count", "top_n", "bottom_n", "average", "sum", "group_by",
           "filter", "show_all", "distinct", "join"]

# keyword seed (EN + RU), used as pseudo-counts so cold start works
_SEED = {
    "count": ["how", "many", "count", "skolko", "kolichestvo", "chislo", "number"],
    "top_n": ["top", "largest", "biggest", "highest", "most", "best", "naibol",
              "maksimal", "luchsh"],
    "bottom_n": ["bottom", "smallest", "lowest", "least", "worst", "naimen",
                 "minimal", "khudsh"],
    "average": ["average", "avg", "mean", "srednee", "sredn"],
    "sum": ["sum", "total", "summa", "itogo", "vsego"],
    "group_by": ["by", "per", "each", "group", "breakdown", "distribution", "po",
                 "razbivka", "raspredelenie"],
    "filter": ["where", "filter", "only", "with", "that", "have", "gde", "tolko",
               "kotorye"],
    "show_all": ["show", "list", "all", "display", "get", "pokazh", "spisok",
                 "vse", "vyvesti"],
    "distinct": ["unique", "distinct", "different", "unikaln", "razlichn"],
    "join": ["join", "combine", "together", "related", "obedinit", "svyazat",
             "vmeste"],
}


def _tokens(text: str) -> list:
    text = re.sub(r"[^\w\s]", " ", (text or "").lower())
    return expand([t for t in text.split() if len(t) > 1])


def intent_of_sql(sql: str) -> str | None:
    """Label the dominant intent of a SQL string by its shape."""
    u = (sql or "").upper()
    if "GROUP BY" in u:
        return "group_by"
    if "COUNT(" in u and "SELECT COUNT" in u:
        return "count"
    if "AVG(" in u:
        return "average"
    if "SUM(" in u:
        return "sum"
    if "DISTINCT" in u:
        return "distinct"
    if "JOIN" in u:
        return "join"
    if "ORDER BY" in u and "LIMIT" in u:
        return "bottom_n" if " ASC" in u else "top_n"
    if "WHERE" in u:
        return "filter"
    if "COUNT(" in u:
        return "count"
    return "show_all"


class IntentClassifier:
    def __init__(self, path=None):
        self.path = Path(path) if path else None
        # counts[intent][token] = occurrences; totals[intent] = token total
        self.counts: dict = {i: {} for i in INTENTS}
        self.totals: dict = {i: 0 for i in INTENTS}
        self.docs: dict = {i: 0 for i in INTENTS}
        self.vocab: set = set()
        if self.path and self.path.exists():
            self.load()
        else:
            self._seed()

    def _seed(self):
        for intent, words in _SEED.items():
            for w in words:
                self.counts[intent][w] = self.counts[intent].get(w, 0) + 2
                self.totals[intent] += 2
                self.vocab.add(w)
            self.docs[intent] += 1

    def partial_fit(self, question: str, intent: str):
        if intent not in self.counts:
            return
        self.docs[intent] += 1
        for tok in _tokens(question):
            self.counts[intent][tok] = self.counts[intent].get(tok, 0) + 1
            self.totals[intent] += 1
            self.vocab.add(tok)
        if self.path:
            self.save()

    def fit(self, examples: list):
        for q, intent in examples:
            self.partial_fit(q, intent)

    def predict(self, question: str):
        toks = _tokens(question)
        if not toks:
            return ("show_all", 0.1)
        total_docs = sum(self.docs.values()) or 1
        v = max(len(self.vocab), 1)
        logp = {}
        for intent in INTENTS:
            lp = math.log((self.docs[intent] + 1) / (total_docs + len(INTENTS)))
            denom = self.totals[intent] + v
            for tok in toks:
                c = self.counts[intent].get(tok, 0)
                lp += math.log((c + 1) / denom)
            logp[intent] = lp
        best = max(logp, key=logp.get)
        # softmax-ish normalized confidence over the top scores
        mx = max(logp.values())
        exps = {i: math.exp(logp[i] - mx) for i in logp}
        prob = exps[best] / sum(exps.values())
        return (best, round(prob, 3))

    def to_dict(self) -> dict:
        return {"counts": self.counts, "totals": self.totals,
                "docs": self.docs, "vocab": sorted(self.vocab)}

    def save(self):
        if not self.path:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(self.to_dict(), ensure_ascii=True),
                             encoding="utf-8")

    def load(self):
        try:
            d = json.loads(self.path.read_text(encoding="utf-8"))
            self.counts = {i: d["counts"].get(i, {}) for i in INTENTS}
            self.totals = {i: d["totals"].get(i, 0) for i in INTENTS}
            self.docs = {i: d["docs"].get(i, 0) for i in INTENTS}
            self.vocab = set(d.get("vocab", []))
        except Exception:
            self._seed()
