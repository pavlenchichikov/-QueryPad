"""Tests for the AI assistant facade (offline paths only - no network)."""


from querypad import ai
from tests.conftest import SAMPLE_SCHEMA


async def test_local_provider_offline(isolated_ml):
    res = await ai.generate_sql(
        question="how many employees", schema=SAMPLE_SCHEMA, provider="local",
    )
    assert res.error is None
    assert "employees" in res.sql


async def test_no_key_falls_back_to_local(isolated_ml, monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    res = await ai.generate_sql(
        question="show all employees", schema=SAMPLE_SCHEMA,
        provider="anthropic", api_key=None,
    )
    # Falls back to local model, never hits the network.
    assert "employees" in res.sql
    assert "fallback" in res.model.lower()


async def test_unknown_provider(isolated_ml):
    res = await ai.generate_sql(
        question="x", schema=SAMPLE_SCHEMA, provider="weird", api_key="k",
    )
    assert res.error is not None


def test_learn_from_execution(isolated_ml):
    ai.learn_from_execution(
        question="count employees",
        sql="SELECT COUNT(*) FROM employees",
        schema=SAMPLE_SCHEMA,
        row_count=5,
    )
    stats = ai.get_local_stats()
    assert stats.get("total_examples", 0) >= 1


def test_learn_from_execution_with_ai_sql_marks_corrected(isolated_ml):
    """The corrections hook: when the executed sql differs from the
    AI-generated sql passed through ai_sql, the learned example records the
    correction (used to upweight it during retrieval)."""
    ai.learn_from_execution(
        question="count active employees",
        sql="SELECT COUNT(*) FROM employees WHERE active = 1",
        schema=SAMPLE_SCHEMA,
        row_count=3,
        ai_sql="SELECT COUNT(*) FROM employees",
    )
    model = ai._get_local_model()
    assert model._history[-1]["corrected"] is True


def test_get_local_stats_shape(isolated_ml):
    stats = ai.get_local_stats()
    assert "total_examples" in stats
