import duckdb

# -----------------------------
# DuckDB helpers
# -----------------------------

def get_existing_columns(con, table):
    """Return existing columns in table or empty dict if table doesn't exist."""
    try:
        result = con.execute(f"PRAGMA table_info('{table}')").fetchall()
        return {row[1]: row[2] for row in result}  # {column_name: column_type}
    except duckdb.CatalogException:
        return {}

def ensure_column(con, table, column_name, column_type):
    """Add column if not exists"""
    con.execute(f"""
        ALTER TABLE {table}
        ADD COLUMN IF NOT EXISTS {column_name} {column_type}
    """)

def ensure_table_and_schema(con, table, schema: dict, pk: str):
    """Create table if missing, add new columns if needed"""
    existing = get_existing_columns(con, table)

    if not existing:
        # Table doesn't exist → create
        cols = ", ".join(f"{c} {t}" for c, t in schema.items())
        con.execute(f"""
            CREATE TABLE {table} (
                {cols},
                PRIMARY KEY ({pk})
            )
        """)
        return

    # Table exists → add missing columns
    for col, dtype in schema.items():
        if col not in existing:
            ensure_column(con, table, col, dtype)

def map_debezium_type_to_duckdb(debezium_type: str) -> str:
    mapping = {
        "int32": "INTEGER",
        "int64": "BIGINT",
        "string": "VARCHAR",
        "boolean": "BOOLEAN",
        "timestamp": "TIMESTAMP"
    }
    return mapping.get(debezium_type, "VARCHAR")

def extract_schema_from_debezium(event: dict) -> dict:
    """
    Extract schema from Debezium payload.
    If schema not present, infer from row.
    """
    if "schema" in event:
        fields = event["schema"]["fields"]
        for field in fields:
            if field["field"] == "after":
                return {
                    col["field"]: map_debezium_type_to_duckdb(col["type"])
                    for col in field["fields"]
                }

    # Fallback: infer schema from 'after' row
    row = event.get("after", {})
    inferred_schema = {}
    for k, v in row.items():
        if isinstance(v, int):
            inferred_schema[k] = "BIGINT"
        elif isinstance(v, float):
            inferred_schema[k] = "DOUBLE"
        elif isinstance(v, bool):
            inferred_schema[k] = "BOOLEAN"
        else:
            inferred_schema[k] = "VARCHAR"
    return inferred_schema

def upsert(con, table, pk, row: dict):
    """Insert or update row using DuckDB's UPSERT pattern"""
    columns = ", ".join(row.keys())
    placeholders = ", ".join(["?"] * len(row))
    update_clause = ", ".join(f"{col}=excluded.{col}" for col in row.keys())

    con.execute(f"""
        INSERT INTO {table} ({columns})
        VALUES ({placeholders})
        ON CONFLICT ({pk}) DO UPDATE SET {update_clause}
    """, list(row.values()))
