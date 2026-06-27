from querypad.ml.extract import extract_slots
from querypad.ml.schema import Schema

SCHEMA = ("TABLE bets: id (INTEGER), sport (TEXT), home_team (TEXT), "
          "away_team (TEXT), pnl (REAL), stake (REAL), date (TEXT)")


def test_order_by_money_word():
    s = Schema.parse(SCHEMA)
    slots = extract_slots("samye dokhodnye stavki", s, s.find_table("bets"))
    assert slots.order_by is not None
    assert slots.order_by[0] == "pnl" and slots.order_by[1] == "DESC"


def test_numeric_filter_operator():
    s = Schema.parse(SCHEMA)
    slots = extract_slots("bets with stake over 100", s, s.find_table("bets"))
    assert ("stake", ">", "100") in slots.filters


def test_entity_filter_like():
    s = Schema.parse(SCHEMA)
    slots = extract_slots("bets where sport is Football", s, s.find_table("bets"))
    assert any(f[0] == "sport" and f[2].strip("%'").lower() == "football"
               for f in slots.filters)


def test_limit_from_top_n():
    s = Schema.parse(SCHEMA)
    slots = extract_slots("top 5 bets by pnl", s, s.find_table("bets"))
    assert slots.limit == 5
