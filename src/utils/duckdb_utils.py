# Helper for schema evolution or DB operations

def get_existing_columns(con, table):
    result = con.execute(f"PRAGMA table_info('{table}')").fetchall()
    return {row[1]: row[2] for row in result}


def ensure_column(con, table, column_name, column_type):
    con.execute(f"""
        ALTER TABLE {table}
        ADD COLUMN IF NOT EXISTS {column_name} {column_type}
    """)


def ensure_table_and_schema(con, table, schema: dict, pk: str):
    existing = get_existing_columns(con, table)

    if not existing:
        cols = ", ".join(f"{c} {t}" for c, t in schema.items())
        con.execute(f"""
            CREATE TABLE {table} (
                {cols},
                PRIMARY KEY ({pk})
            )
        """)
        return

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


def extract_schema_from_debezium(event):
    fields = event["schema"]["fields"]

    for field in fields:
        if field["field"] == "after":
            return {
                col["field"]: map_debezium_type_to_duckdb(col["type"])
                for col in field["fields"]
            }

    return {}


def ensure_table(con, table, schema: dict):
    columns = ", ".join(
        f"{col} {dtype}" for col, dtype in schema.items()
    )
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {table} ({columns})
    """)

def upsert(con, table, pk, row: dict):
    columns = ", ".join(row.keys())
    placeholders = ", ".join(["?"] * len(row))
    update_clause = ", ".join(
        f"{col}=excluded.{col}" for col in row.keys()
    )

    con.execute(f"""
        INSERT INTO {table} ({columns})
        VALUES ({placeholders})
        ON CONFLICT ({pk}) DO UPDATE SET {update_clause}
    """, list(row.values()))


def delete(con, table, pk):
    con.execute(f"DELETE FROM {table} WHERE id = ?", [pk])
