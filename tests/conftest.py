"""Shared fixtures for QueryPad tests."""

import pytest

from querypad import ml_local
from querypad.ml import model as ml_model


@pytest.fixture
def isolated_ml(tmp_path, monkeypatch):
    """Point the local ML model at a temp dir and reset its singleton.

    Keeps tests fully offline and from touching the real ml_data/ directory.
    """
    data_dir = tmp_path / "ml_data"
    data_dir.mkdir()
    monkeypatch.setattr(ml_model, "DATA_DIR", data_dir)
    monkeypatch.setattr(ml_model, "HISTORY_PATH", data_dir / "query_history.jsonl")
    monkeypatch.setattr(ml_model, "INTENT_PATH", data_dir / "intent_model.json")
    monkeypatch.setattr(ml_model, "STATS_PATH", data_dir / "model_stats.json")
    monkeypatch.setattr(ml_model, "_model", None)
    yield ml_local
    monkeypatch.setattr(ml_model, "_model", None)


SAMPLE_SCHEMA = (
    "TABLE employees: id (INTEGER), name (TEXT), department (TEXT), salary (REAL)\n"
    "TABLE departments: name (TEXT), budget (REAL)"
)
