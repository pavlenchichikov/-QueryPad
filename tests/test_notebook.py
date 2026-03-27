"""Tests for the notebook storage."""

import os

import pytest

from querypad.notebook import Cell, Notebook, NotebookStore


@pytest.fixture
def store(tmp_path):
    return NotebookStore(str(tmp_path))


class TestNotebookStore:
    def test_save_and_load(self, store):
        nb = Notebook(id="nb1", name="Test Notebook", cells=[
            Cell(id="c1", cell_type="sql", source="SELECT 1"),
            Cell(id="c2", cell_type="markdown", source="# Hello"),
        ])
        store.save(nb)
        loaded = store.load("nb1")
        assert loaded is not None
        assert loaded.name == "Test Notebook"
        assert len(loaded.cells) == 2
        assert loaded.cells[0].source == "SELECT 1"
        assert loaded.updated_at != ""

    def test_load_nonexistent(self, store):
        assert store.load("nonexistent") is None

    def test_delete(self, store):
        nb = Notebook(id="nb1", name="To Delete")
        store.save(nb)
        store.delete("nb1")
        assert store.load("nb1") is None

    def test_list_all(self, store):
        store.save(Notebook(id="nb1", name="First"))
        store.save(Notebook(id="nb2", name="Second"))
        items = store.list_all()
        assert len(items) == 2
        names = {i["name"] for i in items}
        assert "First" in names
        assert "Second" in names

    def test_list_all_empty(self, store):
        assert store.list_all() == []

    def test_update_notebook(self, store):
        nb = Notebook(id="nb1", name="Original")
        store.save(nb)
        nb.name = "Updated"
        nb.cells.append(Cell(id="c1", cell_type="sql", source="SELECT 42"))
        store.save(nb)
        loaded = store.load("nb1")
        assert loaded.name == "Updated"
        assert len(loaded.cells) == 1

    def test_cell_with_result(self, store):
        nb = Notebook(id="nb1", name="With Results", cells=[
            Cell(id="c1", cell_type="sql", source="SELECT 1",
                 result={"columns": ["1"], "rows": [{"1": 1}], "row_count": 1}),
        ])
        store.save(nb)
        loaded = store.load("nb1")
        assert loaded.cells[0].result is not None
        assert loaded.cells[0].result["row_count"] == 1
