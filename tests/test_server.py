"""Tests for the FastAPI server endpoints."""

import tempfile
import os

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import create_engine, text

from querypad.server import app


@pytest.fixture
def sample_db(tmp_path):
    db_path = tmp_path / "test.db"
    url = f"sqlite:///{db_path}"
    engine = create_engine(url)
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)
        """))
        conn.execute(text("""
            INSERT INTO users (name, age) VALUES ('Alice', 30), ('Bob', 25), ('Charlie', 35)
        """))
        conn.commit()
    return url


@pytest.fixture
async def client():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


@pytest.mark.asyncio
async def test_add_and_list_connections(client, sample_db):
    res = await client.post("/api/connections", json={"name": "TestDB", "url": sample_db})
    assert res.status_code == 200
    data = res.json()
    assert data["name"] == "TestDB"
    conn_id = data["id"]

    res = await client.get("/api/connections")
    assert res.status_code == 200
    assert any(c["id"] == conn_id for c in res.json())


@pytest.mark.asyncio
async def test_get_tables(client, sample_db):
    res = await client.post("/api/connections", json={"id": "t1", "name": "DB", "url": sample_db})
    assert res.status_code == 200

    res = await client.get("/api/connections/t1/tables")
    assert res.status_code == 200
    tables = res.json()
    assert any(t["name"] == "users" for t in tables)


@pytest.mark.asyncio
async def test_run_query(client, sample_db):
    await client.post("/api/connections", json={"id": "t2", "name": "DB", "url": sample_db})

    res = await client.post("/api/query", json={
        "connection_id": "t2",
        "sql": "SELECT * FROM users WHERE age > 28",
    })
    assert res.status_code == 200
    data = res.json()
    assert data["row_count"] == 2
    assert data["error"] is None


@pytest.mark.asyncio
async def test_notebook_crud(client):
    # Create
    res = await client.post("/api/notebooks", json={"name": "Test NB"})
    assert res.status_code == 200
    nb = res.json()
    nb_id = nb["id"]

    # List
    res = await client.get("/api/notebooks")
    assert any(n["id"] == nb_id for n in res.json())

    # Get
    res = await client.get(f"/api/notebooks/{nb_id}")
    assert res.json()["name"] == "Test NB"

    # Update
    res = await client.put(f"/api/notebooks/{nb_id}", json={
        "name": "Updated NB",
        "cells": [{"id": "c1", "cell_type": "sql", "source": "SELECT 1"}],
    })
    assert res.json()["name"] == "Updated NB"

    # Delete
    res = await client.delete(f"/api/notebooks/{nb_id}")
    assert res.json()["ok"] is True


@pytest.mark.asyncio
async def test_settings(client):
    res = await client.get("/api/settings")
    assert res.status_code == 200
    assert "ai_provider" in res.json()

    res = await client.put("/api/settings", json={"ai_provider": "openai"})
    assert res.json()["ok"] is True


@pytest.mark.asyncio
async def test_schema_endpoint(client, sample_db):
    await client.post("/api/connections", json={"id": "t3", "name": "DB", "url": sample_db})
    res = await client.get("/api/connections/t3/schema")
    assert res.status_code == 200
    assert "users" in res.json()["schema"]
