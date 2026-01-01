import os
import json
from pathlib import Path
from dotenv import load_dotenv

"""
Generate Debezium connector configs from schema/table definitions.
Secrets are injected from a local credentials file.
"""

BASE_DIR = Path(__file__).parent
CREDENTIALS_FILE = BASE_DIR.parent / ".credentials"

if not CREDENTIALS_FILE.exists():
    raise FileNotFoundError(
        f"{CREDENTIALS_FILE} not found. "
        "Copy .credentials.example to .credentials and edit it."
    )

load_dotenv(dotenv_path=CREDENTIALS_FILE)

TEMPLATE_FILE = BASE_DIR / "connector_template.json"
CONFIG_FILE = BASE_DIR / "connector_config.json"
OUTPUT_FILE = BASE_DIR / "postgres-cdc-connector.json"

# Load template
template = TEMPLATE_FILE.read_text()

# Load schema/table config
config = json.loads(CONFIG_FILE.read_text())

tables = []

for schema, table_list in config.items():
    for table in table_list:
        tables.append(f"{schema}.{table}")

table_include_list = ",".join(tables)

output = (
    template
    .replace("${POSTGRES_USER}", os.getenv("POSTGRES_USER"))
    .replace("${POSTGRES_PASSWORD}", os.getenv("POSTGRES_PASSWORD"))
    .replace("${POSTGRES_DB}", os.getenv("POSTGRES_DB"))
    .replace("${TABLE_INCLUDE_LIST}", table_include_list)
)

OUTPUT_FILE.write_text(output)

print(f"Connector config generated: {OUTPUT_FILE}")
print(f"Replicating tables: {table_include_list}")
