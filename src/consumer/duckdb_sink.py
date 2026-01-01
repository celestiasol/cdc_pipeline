from kafka import KafkaConsumer
import duckdb
import json
from consumer.config import config
from utils.db_utils import (
    ensure_table_and_schema,
    extract_schema_from_debezium,
    upsert
)

consumer = KafkaConsumer(
    *config["TOPICS"],
    bootstrap_servers=config["KAFKA_BOOTSTRAP_SERVERS"],
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

con = duckdb.connect(config["DUCKDB_PATH"])

for message in consumer:
    event = message.value
    row = event.get("after")
    if not row:
        continue

    # topic: dbserver1.public.users
    _, schema_name, table_name = message.topic.split(".")
    table = f"{schema_name}_{table_name}"

    schema = extract_schema_from_debezium(event)
    pk = "id"  # can be table-specific later

    ensure_table_and_schema(con, table, schema, pk)
    upsert(con, table, pk, row)
