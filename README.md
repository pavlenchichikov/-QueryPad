# QueryPad

![QueryPad](assets/querypad-banner.svg)

A SQL notebook with an AI assistant. Ask a question in plain English and it writes
the query, or write SQL yourself. Works with SQLite, PostgreSQL, MySQL, ClickHouse,
and anything SQLAlchemy connects to.

Cells can be SQL, Markdown or AI. Results show as tables or charts and export to CSV.
Notebooks are saved as JSON.

## Run

```bash
pip install -e .
querypad          # serves http://127.0.0.1:8200
```

Add a connection with a SQLAlchemy URL, e.g. `postgresql://user:pass@host:5432/dbname`
or `sqlite:///path/to/file.db`.

## AI providers

Pick one in Settings:

- **Local ML** - offline, no key, learns from the queries you run
- **Claude** / **GPT** - set `ANTHROPIC_API_KEY` / `OPENAI_API_KEY` (or copy `.env.example` to `.env`)

Without a key it falls back to Local ML. It first reuses a similar past query
(favoring ones that ran and returned rows), adapting the SQL to your schema;
otherwise it classifies the intent (a Naive-Bayes model that learns from your
history), pulls the columns, operators and values out of the question, and builds a
real WHERE / ORDER BY / GROUP BY query, checked against your schema with sqlglot. It
learns from every query you run, including edits you make to its SQL before running.
English and Russian. History lives in `ml_data/`.

Enable **read-only mode** in Settings to reject anything but plain queries
(INSERT / UPDATE / DELETE / DROP) when the AI runs against a real database.

## Layout

`server.py` API, `database.py` connections, `notebook.py` storage,
`ai.py` LLM calls, `ml/` offline model (intent classifier + slot extraction + builder), `static/` UI.

## License

MIT
