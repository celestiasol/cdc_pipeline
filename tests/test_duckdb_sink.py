import unittest, sys
from unittest.mock import Mock, patch

# ---- MOCK kafka BEFORE importing app code ----
sys.modules["kafka"] = unittest.mock.MagicMock()
sys.modules["kafka.errors"] = unittest.mock.MagicMock()
sys.modules["duckdb"] = unittest.mock.MagicMock()

from src.consumer.duckdb_sink import process_message


class TestDuckDBSink(unittest.TestCase):

    def test_skip_message_without_after(self):
        message = Mock()
        message.topic = "dbserver1.public.users"
        message.value = {"payload": {"after": None}}

        con = Mock()

        result = process_message(message, con)

        self.assertFalse(result)

    def test_bad_topic_format(self):
        message = Mock()
        message.topic = "users"
        message.value = {
            "payload": {"after": {"id": 1}}
        }

        con = Mock()

        result = process_message(message, con)

        self.assertFalse(result)

    @patch("src.consumer.duckdb_sink.ensure_table_and_schema")
    @patch("src.consumer.duckdb_sink.upsert")
    @patch(
        "src.consumer.duckdb_sink.extract_schema_from_debezium",
        return_value={"id": "INTEGER", "name": "VARCHAR"}
    )
    def test_upsert_called(
        self,
        mock_extract_schema,
        mock_upsert,
        mock_ensure_table
    ):
        message = Mock()
        message.topic = "dbserver1.public.users"
        message.value = {
            "payload": {
                "after": {"id": 1, "name": "Alice"}
            }
        }

        con = Mock()

        result = process_message(message, con)

        self.assertTrue(result)

        mock_ensure_table.assert_called_once_with(
            con,
            "public_users",
            {"id": "INTEGER", "name": "VARCHAR"},
            "id"
        )

        mock_upsert.assert_called_once_with(
            con,
            "public_users",
            "id",
            {"id": 1, "name": "Alice"}
        )

    @patch("src.consumer.duckdb_sink.ensure_table_and_schema")
    @patch("src.consumer.duckdb_sink.upsert")
    @patch(
        "src.consumer.duckdb_sink.extract_schema_from_debezium",
        return_value={}
    )
    def test_missing_schema_still_upserts(
        self,
        mock_extract_schema,
        mock_upsert,
        mock_ensure_table
    ):
        message = Mock()
        message.topic = "dbserver1.public.users"
        message.value = {
            "payload": {
                "after": {"id": 1, "name": "Alice"}
            }
        }

        con = Mock()

        result = process_message(message, con)

        self.assertTrue(result)
        mock_upsert.assert_called_once()

