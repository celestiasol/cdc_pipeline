import duckdb
import pytest

def test_duckdb_table_creation():
    con = duckdb.connect(":memory:")
    con.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY,
        name VARCHAR,
        email VARCHAR,
        updated_at TIMESTAMP
    )
    """)
    result = con.execute("SELECT * FROM users").fetchall()
    assert result == []
