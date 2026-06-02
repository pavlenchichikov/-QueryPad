"""Tests for the database manager."""


import pytest

from querypad.database import DatabaseManager


@pytest.fixture
def db_manager():
    return DatabaseManager()


@pytest.fixture
def sample_db(tmp_path):
    """Create a sample SQLite database with test data."""
    db_path = tmp_path / "test.db"
    url = f"sqlite:///{db_path}"

    from sqlalchemy import create_engine, text

    engine = create_engine(url)
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE employees (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                department TEXT,
                salary REAL,
                hire_date TEXT
            )
        """))
        conn.execute(text("""
            INSERT INTO employees (name, department, salary, hire_date) VALUES
            ('Alice', 'Engineering', 95000, '2022-01-15'),
            ('Bob', 'Sales', 65000, '2023-03-20'),
            ('Charlie', 'Engineering', 110000, '2021-06-01'),
            ('Diana', 'Marketing', 72000, '2023-08-10'),
            ('Eve', 'Engineering', 88000, '2024-01-05')
        """))
        conn.execute(text("""
            CREATE TABLE departments (
                name TEXT PRIMARY KEY,
                budget REAL
            )
        """))
        conn.execute(text("""
            INSERT INTO departments (name, budget) VALUES
            ('Engineering', 500000),
            ('Sales', 200000),
            ('Marketing', 150000)
        """))
        conn.commit()

    return url


class TestDatabaseManager:
    def test_add_connection(self, db_manager, sample_db):
        info = db_manager.add_connection("test1", "Test DB", sample_db)
        assert info.id == "test1"
        assert info.db_type == "sqlite"

    def test_list_connections(self, db_manager, sample_db):
        db_manager.add_connection("test1", "DB 1", sample_db)
        db_manager.add_connection("test2", "DB 2", sample_db)
        conns = db_manager.list_connections()
        assert len(conns) == 2

    def test_remove_connection(self, db_manager, sample_db):
        db_manager.add_connection("test1", "DB 1", sample_db)
        db_manager.remove_connection("test1")
        assert len(db_manager.list_connections()) == 0

    def test_get_tables(self, db_manager, sample_db):
        db_manager.add_connection("test1", "Test DB", sample_db)
        tables = db_manager.get_tables("test1")
        table_names = {t.name for t in tables}
        assert "employees" in table_names
        assert "departments" in table_names

    def test_get_schema_text(self, db_manager, sample_db):
        db_manager.add_connection("test1", "Test DB", sample_db)
        schema = db_manager.get_schema_text("test1")
        assert "employees" in schema
        assert "name" in schema
        assert "salary" in schema

    def test_execute_select(self, db_manager, sample_db):
        db_manager.add_connection("test1", "Test DB", sample_db)
        result = db_manager.execute_query("test1", "SELECT * FROM employees")
        assert result.error is None
        assert result.row_count == 5
        assert "name" in result.columns

    def test_execute_with_filter(self, db_manager, sample_db):
        db_manager.add_connection("test1", "Test DB", sample_db)
        result = db_manager.execute_query(
            "test1", "SELECT * FROM employees WHERE department = 'Engineering'"
        )
        assert result.row_count == 3

    def test_execute_aggregate(self, db_manager, sample_db):
        db_manager.add_connection("test1", "Test DB", sample_db)
        result = db_manager.execute_query(
            "test1",
            "SELECT department, AVG(salary) as avg_salary FROM employees GROUP BY department",
        )
        assert result.row_count == 3

    def test_execute_join(self, db_manager, sample_db):
        db_manager.add_connection("test1", "Test DB", sample_db)
        result = db_manager.execute_query("test1", """
            SELECT e.name, e.salary, d.budget
            FROM employees e
            JOIN departments d ON e.department = d.name
        """)
        assert result.row_count == 5
        assert "budget" in result.columns

    def test_execute_error(self, db_manager, sample_db):
        db_manager.add_connection("test1", "Test DB", sample_db)
        result = db_manager.execute_query("test1", "SELECT * FROM nonexistent_table")
        assert result.error is not None

    def test_execute_limit(self, db_manager, sample_db):
        db_manager.add_connection("test1", "Test DB", sample_db)
        result = db_manager.execute_query("test1", "SELECT * FROM employees", limit=2)
        assert result.row_count == 2
        assert result.truncated is True

    def test_unknown_connection(self, db_manager):
        with pytest.raises(ValueError):
            db_manager.execute_query("nonexistent", "SELECT 1")


class TestReadOnlyGuard:
    def test_select_allowed(self, db_manager, sample_db):
        db_manager.add_connection("t", "DB", sample_db)
        result = db_manager.execute_query(
            "t", "SELECT * FROM employees", read_only=True
        )
        assert result.error is None
        assert result.row_count == 5

    def test_delete_blocked(self, db_manager, sample_db):
        db_manager.add_connection("t", "DB", sample_db)
        result = db_manager.execute_query(
            "t", "DELETE FROM employees", read_only=True
        )
        assert result.error is not None
        assert "read-only" in result.error.lower()

    def test_delete_allowed_when_off(self, db_manager, sample_db):
        db_manager.add_connection("t", "DB", sample_db)
        result = db_manager.execute_query(
            "t", "DELETE FROM employees WHERE name = 'Bob'", read_only=False
        )
        assert result.error is None


class TestIsWriteSql:
    def test_reads(self):
        from querypad.database import is_write_sql
        assert is_write_sql("SELECT * FROM t") is False
        assert is_write_sql("  with x as (select 1) select * from x") is False
        assert is_write_sql("EXPLAIN SELECT 1") is False

    def test_writes(self):
        from querypad.database import is_write_sql
        assert is_write_sql("INSERT INTO t VALUES (1)") is True
        assert is_write_sql("UPDATE t SET a = 1") is True
        assert is_write_sql("DROP TABLE t") is True
        assert is_write_sql("DELETE FROM t") is True

    def test_cte_wrapped_write_blocked(self):
        from querypad.database import is_write_sql
        assert is_write_sql("WITH x AS (SELECT 1) DELETE FROM t") is True

    def test_stacked_write_blocked(self):
        from querypad.database import is_write_sql
        assert is_write_sql("SELECT 1; DROP TABLE t") is True


class TestCsvExport:
    def test_export_select(self, db_manager, sample_db):
        db_manager.add_connection("t", "DB", sample_db)
        error, csv_text = db_manager.query_to_csv("t", "SELECT name FROM employees")
        assert error is None
        assert "name" in csv_text
        assert "Alice" in csv_text

    def test_export_error(self, db_manager, sample_db):
        db_manager.add_connection("t", "DB", sample_db)
        error, csv_text = db_manager.query_to_csv("t", "SELECT * FROM nope")
        assert error is not None
        assert csv_text == ""
