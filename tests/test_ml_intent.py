from querypad.ml.intent import IntentClassifier, intent_of_sql


def test_intent_of_sql():
    assert intent_of_sql("SELECT COUNT(*) FROM t") == "count"
    assert intent_of_sql("SELECT AVG(x) FROM t") == "average"
    assert intent_of_sql("SELECT * FROM t ORDER BY x DESC LIMIT 5") == "top_n"
    assert intent_of_sql("SELECT a, COUNT(*) FROM t GROUP BY a") == "group_by"


def test_seed_prior_predicts_without_history(tmp_path):
    clf = IntentClassifier(path=tmp_path / "m.json")
    assert clf.predict("how many bets")[0] == "count"
    assert clf.predict("average pnl")[0] == "average"


def test_partial_fit_shifts_and_persists(tmp_path):
    p = tmp_path / "m.json"
    clf = IntentClassifier(path=p)
    for _ in range(8):
        clf.partial_fit("show the schedule of fixtures", "show_all")
    assert clf.predict("show the schedule of fixtures")[0] == "show_all"
    assert p.exists()
    reloaded = IntentClassifier(path=p)
    assert reloaded.predict("show the schedule of fixtures")[0] == "show_all"
