from querypad.ml.synonyms import column_hint, expand


def test_expand_adds_canonicals():
    out = set(expand(["dokhodnye", "stavki"]))
    assert "profit" in out or "money" in out          # RU profitable expands
    assert "stavki" in out                            # original kept


def test_column_hint_money_and_avg():
    assert column_hint("profitable") == "money"
    assert column_hint("dokhod") == "money"
    assert column_hint("srednee") == "avg"
    assert column_hint("randomword") is None
