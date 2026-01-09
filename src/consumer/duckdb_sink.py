from kafka import KafkaConsumer, errors
import duckdb
import json
import time
from src.consumer.config import config
from src.utils.duckdb_utils import ensure_table_and_schema, extract_schema_from_debezium, upsert
from src.utils.logger import get_logger

logger = get_logger("duckdb_sink")


def process_message(message, connect):

    event = message.value
    payload = event.get("payload", {})
    row = payload.get("after")
    if not row:
        return False

    # Topic format: dbserver1.public.users
    try:
        _, schema_name, table_name = message.topic.split(".")
    except ValueError:
        logger.error(f"Unexpected topic format: {message.topic}")
        return False

    table = f"{schema_name}_{table_name}"

    # Extract schema, fallback to inferred schema if missing
    schema = extract_schema_from_debezium(payload)
    if not schema:
        logger.warning(f"Skipping message, schema not found in payload: {payload}")

    pk = "id"  # table-specific primary key

    # Ensure table exists and upsert row
    ensure_table_and_schema(connect, table, schema, pk)
    upsert(connect, table, pk, row)

    logger.info(f"Upserted row into {table}: {row}")
    return True


# -----------------------------
# Connect to Kafka with retries
# -----------------------------
while True:
    try:
        consumer = KafkaConsumer(
            *config["TOPICS"],
            bootstrap_servers=config["KAFKA_BOOTSTRAP_SERVERS"],
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset='earliest',
            group_id="duckdb-sink-group"
        )
        logger.info("Connected to Kafka broker successfully")
        break
    except errors.NoBrokersAvailable:
        logger.warning("Kafka broker not ready, retrying in 5s...")
        time.sleep(5)

# -----------------------------
# Connect to DuckDB
# -----------------------------
con = duckdb.connect(config["DUCKDB_PATH"])
logger.info(f"Connected to DuckDB at {config['DUCKDB_PATH']}")

# -----------------------------
# Consume messages and upsert
# -----------------------------
for message in consumer:
    try:
        process_message(message, con)
    except Exception as e:
        logger.exception(f"Error processing message: {message.value} - {e}")
