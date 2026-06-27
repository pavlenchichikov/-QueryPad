"""Tests for the offline local ML model (public API via the ml_local shim)."""

from querypad.ml import model as ml_model
from tests.conftest import SAMPLE_SCHEMA


def test_count_intent(isolated_ml):
    model = isolated_ml.LocalMLModel()
    res = model.generate("how many employees are there", SAMPLE_SCHEMA)
    assert "COUNT(*)" in res.sql.upper()
    assert "employees" in res.sql


def test_show_all_intent(isolated_ml):
    model = isolated_ml.LocalMLModel()
    res = model.generate("show all employees", SAMPLE_SCHEMA)
    assert res.sql.upper().startswith("SELECT")
    assert "employees" in res.sql


def test_top_n_intent(isolated_ml):
    model = isolated_ml.LocalMLModel()
    res = model.generate("top 5 employees by salary", SAMPLE_SCHEMA)
    assert "ORDER BY" in res.sql.upper()
    assert "DESC" in res.sql.upper()
    assert "5" in res.sql


def test_average_intent(isolated_ml):
    model = isolated_ml.LocalMLModel()
    res = model.generate("average salary", SAMPLE_SCHEMA)
    assert "AVG(" in res.sql.upper()


def test_most_profitable_orders_by_money(isolated_ml):
    """Updated expectation: the slot-filling pipeline now orders by the
    money-hinted numeric column instead of dumping a plain LIMIT 100."""
    model = isolated_ml.LocalMLModel()
    res = model.generate("show me the most profitable employees", SAMPLE_SCHEMA)
    assert "ORDER BY salary DESC" in res.sql
    assert "employees" in res.sql


def test_empty_question(isolated_ml):
    model = isolated_ml.LocalMLModel()
    res = model.generate("   ", SAMPLE_SCHEMA)
    assert res.error is not None


def test_no_tables(isolated_ml):
    model = isolated_ml.LocalMLModel()
    res = model.generate("how many", "")
    assert res.error is not None


def test_learn_persists_and_grows(isolated_ml):
    model = isolated_ml.LocalMLModel()
    assert len(model._history) == 0
    model.learn(
        question="count of orders",
        sql="SELECT COUNT(*) FROM orders",
        schema="TABLE orders: id (INTEGER)",
    )
    assert len(model._history) == 1
    assert ml_model.HISTORY_PATH.exists()


def test_learn_skips_duplicates(isolated_ml):
    model = isolated_ml.LocalMLModel()
    for _ in range(3):
        model.learn(
            question="same question",
            sql="SELECT 1",
            schema="TABLE t: id (INTEGER)",
        )
    assert len(model._history) == 1


def test_similarity_recall(isolated_ml):
    model = isolated_ml.LocalMLModel()
    model.learn(
        question="list all engineers in engineering",
        sql="SELECT * FROM employees WHERE department = 'Engineering'",
        schema=SAMPLE_SCHEMA,
    )
    res = model.generate("list all engineers in engineering", SAMPLE_SCHEMA)
    assert res.source == "similarity"
    assert res.confidence >= 0.65


def test_stats(isolated_ml):
    model = isolated_ml.LocalMLModel()
    model.learn("count rows", "SELECT COUNT(*) FROM t", "TABLE t: id (INTEGER)")
    stats = model.get_stats()
    assert stats["total_examples"] == 1
    assert "intent_distribution" in stats


def test_learn_with_ai_sql_marks_corrected(isolated_ml):
    """Public-facing corrections hook: when the executed sql differs from the
    AI-generated sql, the example is flagged as corrected (boosts retrieval
    weight); when it matches, it is not."""
    model = isolated_ml.LocalMLModel()
    model.learn(
        question="count of orders",
        sql="SELECT COUNT(*) FROM orders WHERE id > 0",
        schema="TABLE orders: id (INTEGER)",
        was_executed=True,
        row_count=3,
        ai_sql="SELECT COUNT(*) FROM orders",
    )
    assert model._history[0]["corrected"] is True

    model.learn(
        question="count of orders again",
        sql="SELECT COUNT(*) FROM orders",
        schema="TABLE orders: id (INTEGER)",
        was_executed=True,
        row_count=3,
        ai_sql="SELECT COUNT(*) FROM orders",
    )
    assert model._history[1]["corrected"] is False
