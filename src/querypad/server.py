"""FastAPI server - REST API for QueryPad."""

from __future__ import annotations

import os
import uuid
from contextlib import asynccontextmanager
from dataclasses import asdict
from pathlib import Path
from typing import Any

from fastapi import FastAPI
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from querypad.ai import generate_sql, get_local_stats, learn_from_execution
from querypad.database import DatabaseManager, is_write_sql
from querypad.notebook import Cell, Notebook, NotebookStore

STATIC_DIR = Path(__file__).parent / "static"

db_manager = DatabaseManager()
nb_store = NotebookStore()

# Settings persisted in memory (can be extended to file)
_settings: dict[str, Any] = {
    "ai_provider": "anthropic",
    "ai_api_key": os.environ.get("ANTHROPIC_API_KEY") or os.environ.get("OPENAI_API_KEY", ""),
    "ai_model": "",
    "read_only": False,
}


@asynccontextmanager
async def lifespan(application: FastAPI):
    os.makedirs("uploads", exist_ok=True)
    yield


app = FastAPI(title="QueryPad", version="0.1.0", lifespan=lifespan)


# Connection management

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


# Query execution

@app.post("/api/query")
async def run_query(payload: dict[str, Any]):
    conn_id = payload["connection_id"]
    sql = payload["sql"]
    limit = payload.get("limit", 500)
    result = db_manager.execute_query(
        conn_id, sql, limit=limit, read_only=bool(_settings.get("read_only")),
    )
    return asdict(result)


@app.post("/api/query/export")
async def export_query(payload: dict[str, Any]):
    """Run a query and stream the full result set as a CSV download."""
    conn_id = payload["connection_id"]
    sql = payload["sql"]
    if bool(_settings.get("read_only")) and is_write_sql(sql):
        return {"error": "Read-only mode is on: only SELECT-style queries are allowed."}
    error, csv_text = db_manager.query_to_csv(conn_id, sql)
    if error:
        return {"error": error}
    return StreamingResponse(
        iter([csv_text]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=querypad_export.csv"},
    )


# AI assistant

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


@app.post("/api/ai/learn")
async def ai_learn(payload: dict[str, Any]):
    """Teach local ML model from a successful query execution."""
    conn_id = payload.get("connection_id", "")
    question = payload.get("question", "")
    sql = payload.get("sql", "")
    row_count = payload.get("row_count", 0)
    ai_sql = payload.get("ai_sql")

    if not question or not sql:
        return {"ok": False, "error": "question and sql required"}

    schema = ""
    dialect = "sqlite"
    if conn_id:
        try:
            schema = db_manager.get_schema_text(conn_id)
            conn_info = db_manager._connections.get(conn_id)
            dialect = conn_info.db_type if conn_info else "sqlite"
        except Exception:
            pass

    learn_from_execution(
        question=question, sql=sql, schema=schema,
        dialect=dialect, row_count=row_count, ai_sql=ai_sql,
    )
    return {"ok": True}


@app.get("/api/ai/stats")
async def ai_stats():
    """Return local ML model statistics."""
    return get_local_stats()


# Notebook management

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


# Settings

@app.get("/api/settings")
async def get_settings():
    safe = dict(_settings)
    key = safe.get("ai_api_key")
    if key:
        safe["ai_api_key"] = key[:8] + "..." if len(key) > 8 else "***"
    return safe


@app.put("/api/settings")
async def update_settings(payload: dict[str, Any]):
    for key in ("ai_provider", "ai_api_key", "ai_model"):
        if key in payload:
            _settings[key] = payload[key]
    if "read_only" in payload:
        _settings["read_only"] = bool(payload["read_only"])
    return {"ok": True}


# Static files

@app.get("/")
async def index():
    return FileResponse(STATIC_DIR / "index.html")


app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
