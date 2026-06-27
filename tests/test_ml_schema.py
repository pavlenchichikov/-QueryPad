from querypad.ml.schema import Schema

SCHEMA = (
    "TABLE bets: id (INTEGER), date (TEXT), sport (TEXT), home_team (TEXT), "
    "away_team (TEXT), pnl (REAL), edge (REAL), stake (REAL)\n"
    "TABLE odds: id (INTEGER), event_id (TEXT), home_team (TEXT), bookmaker (TEXT)"
)


def test_parse_tables_and_types():
    s = Schema.parse(SCHEMA)
    assert {t.name for t in s.tables} == {"bets", "odds"}
    bets = s.find_table("bets")
    assert bets.column("pnl").is_numeric
    assert bets.column("sport").is_text
    assert bets.column("date").is_date


def test_find_table_by_name_and_column():
    s = Schema.parse(SCHEMA)
    assert s.find_table("show me the bets").name == "bets"
    assert s.find_table("bookmaker odds").name == "odds"


def test_find_column_numeric_pref_skips_id():
    s = Schema.parse(SCHEMA)
    bets = s.find_table("bets")
    col = s.find_column(bets, "most profitable", prefer_numeric=True)
    assert col is not None and col.name != "id"


def test_join_keys_shared_column():
    s = Schema.parse(SCHEMA)
    k = s.join_keys(s.find_table("bets"), s.find_table("odds"))
    assert k in (("home_team", "home_team"), ("id", "id"))


def test_has():
    s = Schema.parse(SCHEMA)
    assert s.has("bets", "pnl") and not s.has("bets", "nope") and not s.has("ghost")
