"""Notebook storage — JSON-based cell notebooks."""

from __future__ import annotations

import json
import os
import time
from dataclasses import asdict, dataclass, field


@dataclass
class Cell:
    id: str
    cell_type: str  # "sql", "markdown", "ai"
    source: str = ""
    result: dict | None = None
    created_at: str = ""
    connection_id: str = ""


@dataclass
class Notebook:
    id: str
    name: str
    cells: list[Cell] = field(default_factory=list)
    created_at: str = ""
    updated_at: str = ""
    default_connection: str = ""


NOTEBOOKS_DIR = "notebooks"


class NotebookStore:
    """Persist notebooks as JSON files."""

    def __init__(self, base_dir: str = NOTEBOOKS_DIR):
        self.base_dir = base_dir
        os.makedirs(base_dir, exist_ok=True)

    def _path(self, notebook_id: str) -> str:
        return os.path.join(self.base_dir, f"{notebook_id}.json")

    def save(self, nb: Notebook) -> Notebook:
        nb.updated_at = time.strftime("%Y-%m-%d %H:%M:%S")
        if not nb.created_at:
            nb.created_at = nb.updated_at
        with open(self._path(nb.id), "w", encoding="utf-8") as f:
            json.dump(asdict(nb), f, indent=2, ensure_ascii=False)
        return nb

    def load(self, notebook_id: str) -> Notebook | None:
        path = self._path(notebook_id)
        if not os.path.exists(path):
            return None
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        cells = [Cell(**c) for c in data.pop("cells", [])]
        return Notebook(**data, cells=cells)

    def delete(self, notebook_id: str):
        path = self._path(notebook_id)
        if os.path.exists(path):
            os.remove(path)

    def list_all(self) -> list[dict]:
        result = []
        for fname in sorted(os.listdir(self.base_dir)):
            if fname.endswith(".json"):
                path = os.path.join(self.base_dir, fname)
                with open(path, encoding="utf-8") as f:
                    data = json.load(f)
                result.append({
                    "id": data["id"],
                    "name": data["name"],
                    "cells_count": len(data.get("cells", [])),
                    "created_at": data.get("created_at", ""),
                    "updated_at": data.get("updated_at", ""),
                })
        return result
