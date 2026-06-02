# QueryPad

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

Without a key it falls back to Local ML. The local model matches your question against
past ones (TF-IDF + cosine similarity), or failing that maps the detected intent
(count, top-N, average, group-by) onto your schema. History lives in `ml_data/`.

Enable **read-only mode** in Settings to reject anything but plain queries
(INSERT / UPDATE / DELETE / DROP) when the AI runs against a real database.

## Layout

`server.py` API, `database.py` connections, `notebook.py` storage,
`ai.py` LLM calls, `ml_local.py` offline model, `static/` UI.

## License

MIT
