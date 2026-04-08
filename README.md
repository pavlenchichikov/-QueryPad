# QueryPad

SQL notebook with a built-in AI assistant. You type a question in plain English, it writes the query. Or just write SQL yourself — either way, results show up instantly.

Works with SQLite, PostgreSQL, MySQL, ClickHouse, and anything SQLAlchemy can connect to.

## What it does

- Notebooks with SQL, Markdown, and AI cells mixed together
- AI generates SQL from natural language (Claude, GPT, or a local offline model)
- The local model learns from every query you run — no API key needed
- Schema browser so you don't have to memorize table names
- Charts (bar, line, pie, doughnut) straight from query results
- Notebooks auto-save as JSON

## Getting started

```bash
pip install -e .
querypad
```

Then open http://127.0.0.1:8200.

## Connect a database

Click **Add Connection** and paste a SQLAlchemy URL:

```
sqlite:///path/to/file.db
postgresql://user:pass@host:5432/dbname
mysql+pymysql://user:pass@host:3306/dbname
clickhouse://user:pass@host:8123/dbname
```

## AI setup

Three options under **Settings**:

- **Local ML** — works offline, no keys, learns as you go
- **Claude** — needs `ANTHROPIC_API_KEY`
- **GPT** — needs `OPENAI_API_KEY`

If no key is set, it falls back to the local model automatically. Online providers also feed successful results back into the local model, so it keeps improving either way.

## How the local model works

1. Checks past queries for similar questions (TF-IDF + cosine similarity)
2. If nothing matches well, detects intent (count, top N, average, group by, etc.) and maps it to your schema
3. Last resort — `SELECT * FROM table LIMIT 100`

Training data lives in `ml_data/` and grows on its own.

## Project structure

```
src/querypad/
  server.py       — FastAPI app
  database.py     — connection manager
  notebook.py     — notebook storage
  ai.py           — LLM integration
  ml_local.py     — local ML model
  static/         — web UI
```

## License

MIT
