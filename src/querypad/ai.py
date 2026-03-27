"""AI assistant — natural language to SQL translation."""

from __future__ import annotations

import os
from dataclasses import dataclass

import httpx

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


async def generate_sql(
    question: str,
    schema: str,
    dialect: str = "sqlite",
    provider: str = "anthropic",
    api_key: str | None = None,
    model: str | None = None,
) -> AIResponse:
    """Convert a natural language question to SQL using an LLM."""

    key = api_key or os.environ.get("ANTHROPIC_API_KEY") or os.environ.get("OPENAI_API_KEY", "")
    if not key:
        return AIResponse(sql="", model="", error="No API key configured. Set ANTHROPIC_API_KEY or OPENAI_API_KEY, or provide it in Settings.")

    user_msg = f"""Database dialect: {dialect}

Schema:
{schema}

Question: {question}

Write the SQL query:"""

    if provider == "anthropic":
        return await _call_anthropic(key, user_msg, model or "claude-sonnet-4-20250514")
    elif provider == "openai":
        return await _call_openai(key, user_msg, model or "gpt-4o-mini")
    else:
        return AIResponse(sql="", model="", error=f"Unknown provider: {provider}")


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
