import unittest
from unittest.mock import patch

import pandas as pd

from ingestion.destinations.databricks import databricks


class BronzeBatchWriterTests(unittest.TestCase):
    def test_write_bronze_batches_forwards_identifiers_and_counts_rows(self) -> None:
        batches = (
            (f"2025-{number}", pd.DataFrame({"value": [number]}))
            for number in (1, 2)
        )

        with (
            patch.object(
                databricks,
                "bronze_write",
                side_effect=[
                    {"row_count": 1, "table": "bronze.test.table"},
                    {"row_count": 2, "table": "bronze.test.table"},
                ],
            ) as bronze_write,
            patch("builtins.print"),
        ):
            row_count = databricks.write_bronze_batches(
                batches,
                catalog="bronze",
                schema="test",
                table="table",
                key_cols=["_source_sha256"],
                volume_path="/Volumes/bronze/test/files",
            )

        self.assertEqual(row_count, 3)
        self.assertEqual(bronze_write.call_count, 2)
        self.assertEqual(bronze_write.call_args_list[0].kwargs["batch_id"], "2025-1")
        self.assertEqual(bronze_write.call_args_list[1].kwargs["batch_id"], "2025-2")
        self.assertEqual(
            bronze_write.call_args_list[0].kwargs["volume_path"],
            "/Volumes/bronze/test/files",
        )

    def test_write_bronze_batches_returns_zero_for_empty_iterator(self) -> None:
        with patch.object(databricks, "bronze_write") as bronze_write:
            row_count = databricks.write_bronze_batches(
                iter(()),
                catalog="bronze",
                schema="test",
                table="table",
                key_cols=["_source_sha256"],
            )

        self.assertEqual(row_count, 0)
        bronze_write.assert_not_called()


if __name__ == "__main__":
    unittest.main()
