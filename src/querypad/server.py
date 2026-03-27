"""FastAPI server — REST API for QueryPad."""

from __future__ import annotations

import os
import uuid
from contextlib import asynccontextmanager
from dataclasses import asdict
from pathlib import Path
from typing import Any

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from querypad.ai import generate_sql
from querypad.database import DatabaseManager
from querypad.notebook import Cell, Notebook, NotebookStore

STATIC_DIR = Path(__file__).parent / "static"

db_manager = DatabaseManager()
nb_store = NotebookStore()

# Settings persisted in memory (can be extended to file)
_settings: dict[str, str] = {
    "ai_provider": "anthropic",
    "ai_api_key": os.environ.get("ANTHROPIC_API_KEY") or os.environ.get("OPENAI_API_KEY", ""),
    "ai_model": "",
}


@asynccontextmanager
async def lifespan(application: FastAPI):
    os.makedirs("uploads", exist_ok=True)
    yield


app = FastAPI(title="QueryPad", version="0.1.0", lifespan=lifespan)


# ── Connection management ───────────────────────────────────────

@app.get("/api/connections")
async def list_connections():
    return [asdict(c) for c in db_manager.list_connections()]


@app.post("/api/connections")
async def add_connection(payload: dict[str, Any]):
    conn_id = payload.get("id", str(uuid.uuid4())[:8])
    info = db_manager.add_connection(conn_id, payload["name"], payload["url"])
    return asdict(info)


@app.delete("/api/connections/{conn_id}")
async def remove_connection(conn_id: str):
    db_manager.remove_connection(conn_id)
    return {"ok": True}


@app.get("/api/connections/{conn_id}/tables")
async def get_tables(conn_id: str):
    tables = db_manager.get_tables(conn_id)
    return [asdict(t) for t in tables]


@app.get("/api/connections/{conn_id}/schema")
async def get_schema(conn_id: str):
    return {"schema": db_manager.get_schema_text(conn_id)}


# ── Query execution ─────────────────────────────────────────────

@app.post("/api/query")
async def run_query(payload: dict[str, Any]):
    conn_id = payload["connection_id"]
    sql = payload["sql"]
    limit = payload.get("limit", 500)
    result = db_manager.execute_query(conn_id, sql, limit=limit)
    return asdict(result)


# ── AI assistant ────────────────────────────────────────────────

@app.post("/api/ai/generate")
async def ai_generate(payload: dict[str, Any]):
    conn_id = payload["connection_id"]
    question = payload["question"]
    schema = db_manager.get_schema_text(conn_id)
    conn_info = db_manager._connections.get(conn_id)
    dialect = conn_info.db_type if conn_info else "sqlite"

    result = await generate_sql(
        question=question,
        schema=schema,
        dialect=dialect,
        provider=_settings.get("ai_provider", "anthropic"),
        api_key=_settings.get("ai_api_key", ""),
        model=_settings.get("ai_model") or None,
    )
    return asdict(result)


# ── Notebook management ─────────────────────────────────────────

@app.get("/api/notebooks")
async def list_notebooks():
    return nb_store.list_all()


@app.post("/api/notebooks")
async def create_notebook(payload: dict[str, Any]):
    nb_id = payload.get("id", str(uuid.uuid4())[:8])
    nb = Notebook(
        id=nb_id,
        name=payload.get("name", "Untitled"),
        default_connection=payload.get("connection_id", ""),
    )
    nb_store.save(nb)
    return asdict(nb)


@app.get("/api/notebooks/{nb_id}")
async def get_notebook(nb_id: str):
    nb = nb_store.load(nb_id)
    if not nb:
        return {"error": "Notebook not found"}
    return asdict(nb)


@app.put("/api/notebooks/{nb_id}")
async def update_notebook(nb_id: str, payload: dict[str, Any]):
    cells = [Cell(**c) for c in payload.get("cells", [])]
    nb = Notebook(
        id=nb_id,
        name=payload.get("name", "Untitled"),
        cells=cells,
        default_connection=payload.get("default_connection", ""),
        created_at=payload.get("created_at", ""),
    )
    nb_store.save(nb)
    return asdict(nb)


@app.delete("/api/notebooks/{nb_id}")
async def delete_notebook(nb_id: str):
    nb_store.delete(nb_id)
    return {"ok": True}


# ── Settings ────────────────────────────────────────────────────

@app.get("/api/settings")
async def get_settings():
    safe = dict(_settings)
    if safe.get("ai_api_key"):
        safe["ai_api_key"] = safe["ai_api_key"][:8] + "..." if len(safe["ai_api_key"]) > 8 else "***"
    return safe


@app.put("/api/settings")
async def update_settings(payload: dict[str, Any]):
    for key in ("ai_provider", "ai_api_key", "ai_model"):
        if key in payload:
            _settings[key] = payload[key]
    return {"ok": True}


# ── Static files ────────────────────────────────────────────────

@app.get("/")
async def index():
    return FileResponse(STATIC_DIR / "index.html")


app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
