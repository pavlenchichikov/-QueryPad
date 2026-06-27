from querypad.ml.validate import adapt, validate

SCHEMA = "TABLE bets: id (INTEGER), sport (TEXT), pnl (REAL)"


def test_validate_keeps_good_sql():
    sql, ok, note = validate("SELECT pnl FROM bets", SCHEMA)
    assert ok and "bets" in sql


def test_validate_flags_unknown_column():
    sql, ok, note = validate("SELECT ghost FROM bets", SCHEMA)
    assert ok is False and "ghost" in note


def test_validate_transpiles_limit_to_tsql():
    sql, ok, note = validate("SELECT * FROM bets LIMIT 5", SCHEMA, dialect="mssql")
    assert "TOP" in sql.upper() or "FETCH" in sql.upper()


def test_adapt_renames_table_and_column():
    old = "TABLE bets: id (INTEGER), pnl (REAL)"
    new = "TABLE wagers: id (INTEGER), profit (REAL)"
    out = adapt("SELECT pnl FROM bets", old, new)
    assert "wagers" in out
