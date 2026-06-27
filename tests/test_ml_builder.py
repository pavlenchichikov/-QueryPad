from querypad.ml.builder import build
from querypad.ml.extract import Slots
from querypad.ml.schema import Schema

SCHEMA = ("TABLE bets: id (INTEGER), sport (TEXT), home_team (TEXT), "
          "away_team (TEXT), pnl (REAL), stake (REAL)")


def test_build_count():
    s = Schema.parse(SCHEMA)
    sql = build("count", s.find_table("bets"), s, Slots())
    assert sql == "SELECT COUNT(*) AS count FROM bets"


def test_build_top_n_with_order_and_limit():
    s = Schema.parse(SCHEMA)
    slots = Slots(order_by=("pnl", "DESC"), limit=5)
    sql = build("top_n", s.find_table("bets"), s, slots)
    assert "ORDER BY pnl DESC" in sql and "LIMIT 5" in sql


def test_build_filter_where():
    s = Schema.parse(SCHEMA)
    slots = Slots(filters=[("stake", ">", "100")])
    sql = build("filter", s.find_table("bets"), s, slots)
    assert "WHERE stake > 100" in sql


def test_build_group_by_numeric_agg():
    s = Schema.parse(SCHEMA)
    sql = build("group_by", s.find_table("bets"), s, Slots(target_columns=["sport"]))
    assert "GROUP BY sport" in sql and "FROM bets" in sql
