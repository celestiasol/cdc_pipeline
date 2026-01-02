from kafka import KafkaConsumer, errors
import duckdb
import json
import time
from src.consumer.config import config
from src.utils.duckdb_utils import (
    ensure_table_and_schema,
    extract_schema_from_debezium,
    upsert
)


while True:
    try:
        consumer = KafkaConsumer(
            *config["TOPICS"],
            bootstrap_servers=config["KAFKA_BOOTSTRAP_SERVERS"],
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset='earliest'
        )
        print("Connected to Kafka broker")
        break
    except errors.NoBrokersAvailable:
        print("Kafka not ready, retrying in 5s...")
        time.sleep(5)

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
