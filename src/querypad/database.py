"""Database connection manager and schema introspection."""

from __future__ import annotations

import time
from dataclasses import dataclass, field

import pandas as pd
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine


@dataclass
class QueryResult:
    columns: list[str]
    rows: list[dict]
    row_count: int
    elapsed_ms: float
    error: str | None = None
    truncated: bool = False


@dataclass
class TableInfo:
    name: str
    columns: list[dict]
    row_count: int | None = None


@dataclass
class ConnectionInfo:
    id: str
    name: str
    url: str
    db_type: str


class DatabaseManager:
    """Manages multiple database connections."""

    def __init__(self):
        self._engines: dict[str, Engine] = {}
        self._connections: dict[str, ConnectionInfo] = {}

    def add_connection(self, conn_id: str, name: str, url: str) -> ConnectionInfo:
        engine = create_engine(url)
        # Test the connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        self._engines[conn_id] = engine
        db_type = engine.dialect.name
        info = ConnectionInfo(id=conn_id, name=name, url=url, db_type=db_type)
        self._connections[conn_id] = info
        return info

    def remove_connection(self, conn_id: str):
        if conn_id in self._engines:
            self._engines[conn_id].dispose()
            del self._engines[conn_id]
        self._connections.pop(conn_id, None)

    def list_connections(self) -> list[ConnectionInfo]:
        return list(self._connections.values())

    def get_engine(self, conn_id: str) -> Engine:
        engine = self._engines.get(conn_id)
        if not engine:
            raise ValueError(f"Connection '{conn_id}' not found")
        return engine

    def get_tables(self, conn_id: str) -> list[TableInfo]:
        engine = self.get_engine(conn_id)
        insp = inspect(engine)
        tables = []
        for table_name in insp.get_table_names():
            columns = []
            for col in insp.get_columns(table_name):
                columns.append({
                    "name": col["name"],
                    "type": str(col["type"]),
                    "nullable": col.get("nullable", True),
                })
            tables.append(TableInfo(name=table_name, columns=columns))
        return tables

    def get_schema_text(self, conn_id: str) -> str:
        """Return a text description of the schema for LLM context."""
        tables = self.get_tables(conn_id)
        lines = []
        for t in tables:
            cols = ", ".join(f"{c['name']} ({c['type']})" for c in t.columns)
            lines.append(f"TABLE {t.name}: {cols}")
        return "\n".join(lines)

    def execute_query(self, conn_id: str, sql: str, limit: int = 500) -> QueryResult:
        engine = self.get_engine(conn_id)
        t0 = time.perf_counter()
        try:
            with engine.connect() as conn:
                result = conn.execute(text(sql))
                if result.returns_rows:
                    df = pd.DataFrame(result.fetchall(), columns=list(result.keys()))
                    truncated = len(df) > limit
                    if truncated:
                        df = df.head(limit)
                    elapsed = round((time.perf_counter() - t0) * 1000, 2)
                    return QueryResult(
                        columns=list(df.columns),
                        rows=df.fillna("").to_dict(orient="records"),
                        row_count=len(df),
                        elapsed_ms=elapsed,
                        truncated=truncated,
                    )
                else:
                    conn.commit()
                    elapsed = round((time.perf_counter() - t0) * 1000, 2)
                    return QueryResult(
                        columns=[],
                        rows=[],
                        row_count=result.rowcount or 0,
                        elapsed_ms=elapsed,
                    )
        except Exception as exc:
            elapsed = round((time.perf_counter() - t0) * 1000, 2)
            return QueryResult(
                columns=[], rows=[], row_count=0,
                elapsed_ms=elapsed, error=str(exc),
            )
