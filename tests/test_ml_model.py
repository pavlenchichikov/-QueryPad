"""End-to-end tests for the Local-ML orchestrator (querypad.ml.model)."""

import querypad.ml.model as M  # noqa: N812

SCHEMA = ("TABLE bets: id (INTEGER), sport (TEXT), home_team (TEXT), "
          "away_team (TEXT), pnl (REAL), stake (REAL), date (TEXT)\n"
          "TABLE mma_events: api_event_id (INTEGER), venue (TEXT), city (TEXT)")


def _fresh(tmp_path, monkeypatch):
    monkeypatch.setattr(M, "DATA_DIR", tmp_path)
    monkeypatch.setattr(M, "HISTORY_PATH", tmp_path / "h.jsonl")
    monkeypatch.setattr(M, "INTENT_PATH", tmp_path / "intent.json")
    monkeypatch.setattr(M, "STATS_PATH", tmp_path / "stats.json")
    return M.LocalMLModel()


def test_profitable_bets_orders_by_money(tmp_path, monkeypatch):
    ml = _fresh(tmp_path, monkeypatch)
    r = ml.generate("samye dokhodnye stavki", SCHEMA)
    assert "FROM bets" in r.sql and "ORDER BY pnl DESC" in r.sql


def test_team_question_filters_not_groupby_mma(tmp_path, monkeypatch):
    ml = _fresh(tmp_path, monkeypatch)
    r = ml.generate("stavki na Inter vs Roma", SCHEMA)
    assert "FROM bets" in r.sql
    assert "GROUP BY" not in r.sql.upper()
    assert "WHERE" in r.sql.upper()


def test_learn_and_retrieve(tmp_path, monkeypatch):
    ml = _fresh(tmp_path, monkeypatch)
    ml.learn("count all bets", "SELECT COUNT(*) AS count FROM bets", SCHEMA,
             was_executed=True, row_count=1)
    r = ml.generate("count all bets", SCHEMA)
    assert "COUNT(*)" in r.sql.upper()
