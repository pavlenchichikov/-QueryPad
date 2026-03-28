"""AI assistant — natural language to SQL translation."""

from __future__ import annotations

import os
from dataclasses import dataclass, field

import httpx

from querypad.ml_local import get_model as _get_local_model

SYSTEM_PROMPT = """You are a SQL expert assistant. Given a database schema and a user's question in natural language, generate a SQL query that answers the question.

Rules:
- Return ONLY the SQL query, no explanations
- Use standard SQL syntax compatible with the specified database dialect
- If the question is ambiguous, make reasonable assumptions
- Always use table and column names exactly as they appear in the schema
- Limit results to 100 rows unless the user asks for more
- For aggregations, include meaningful column aliases
"""


@dataclass
class AIResponse:
    sql: str
    model: str
    error: str | None = None
    confidence: float = 0.0
    source: str = ""
    similar_questions: list = field(default_factory=list)


async def generate_sql(
    question: str,
    schema: str,
    dialect: str = "sqlite",
    provider: str = "anthropic",
    api_key: str | None = None,
    model: str | None = None,
) -> AIResponse:
    """Convert a natural language question to SQL using an LLM or local ML."""

    # Local ML — no API key needed
    if provider == "local":
        return _generate_local(question, schema, dialect)

    key = api_key or os.environ.get("ANTHROPIC_API_KEY") or os.environ.get("OPENAI_API_KEY", "")
    if not key:
        # Fallback to local ML when no API key
        result = _generate_local(question, schema, dialect)
        if result.sql:
            result.model += " (auto-fallback, no API key)"
            return result
        return AIResponse(sql="", model="", error="No API key configured and local ML couldn't generate SQL. Set ANTHROPIC_API_KEY or OPENAI_API_KEY in Settings, or use Local ML provider.")

    user_msg = f"""Database dialect: {dialect}

Schema:
{schema}

Question: {question}

Write the SQL query:"""

    if provider == "anthropic":
        result = await _call_anthropic(key, user_msg, model or "claude-sonnet-4-20250514")
    elif provider == "openai":
        result = await _call_openai(key, user_msg, model or "gpt-4o-mini")
    else:
        return AIResponse(sql="", model="", error=f"Unknown provider: {provider}")

    # Learn from successful API responses for local model
    if result.sql and not result.error:
        try:
            ml = _get_local_model()
            ml.learn(question=question, sql=result.sql, schema=schema, dialect=dialect)
        except Exception:
            pass

    return result


def _generate_local(question: str, schema: str, dialect: str) -> AIResponse:
    """Generate SQL using local ML model."""
    try:
        ml = _get_local_model()
        result = ml.generate(question=question, schema=schema, dialect=dialect)
        return AIResponse(
            sql=result.sql,
            model=result.model,
            error=result.error,
            confidence=result.confidence,
            source=result.source,
            similar_questions=result.similar_questions,
        )
    except Exception as exc:
        return AIResponse(sql="", model="local-ml", error=f"Local ML error: {exc}")


def learn_from_execution(
    question: str, sql: str, schema: str, dialect: str = "sqlite",
    row_count: int = 0,
):
    """Teach the local model from a successfully executed query."""
    try:
        ml = _get_local_model()
        ml.learn(
            question=question, sql=sql, schema=schema, dialect=dialect,
            was_executed=True, row_count=row_count,
        )
    except Exception:
        pass


def get_local_stats() -> dict:
    """Return local ML model statistics."""
    try:
        return _get_local_model().get_stats()
    except Exception:
        return {"error": "Local ML not available"}


async def _call_anthropic(api_key: str, user_msg: str, model: str) -> AIResponse:
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            res = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": api_key,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": model,
                    "max_tokens": 1024,
                    "system": SYSTEM_PROMPT,
                    "messages": [{"role": "user", "content": user_msg}],
                },
            )
            res.raise_for_status()
            data = res.json()
            sql = data["content"][0]["text"].strip()
            # Strip markdown code fences if present
            if sql.startswith("```"):
                lines = sql.splitlines()
                sql = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])
            return AIResponse(sql=sql, model=model)
    except Exception as exc:
        return AIResponse(sql="", model=model, error=str(exc))


async def _call_openai(api_key: str, user_msg: str, model: str) -> AIResponse:
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            res = await client.post(
                "https://api.openai.com/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": model,
                    "messages": [
                        {"role": "system", "content": SYSTEM_PROMPT},
                        {"role": "user", "content": user_msg},
                    ],
                    "max_tokens": 1024,
                    "temperature": 0,
                },
            )
            res.raise_for_status()
            data = res.json()
            sql = data["choices"][0]["message"]["content"].strip()
            if sql.startswith("```"):
                lines = sql.splitlines()
                sql = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])
            return AIResponse(sql=sql, model=model)
    except Exception as exc:
        return AIResponse(sql="", model=model, error=str(exc))
