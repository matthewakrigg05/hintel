import unittest
from unittest.mock import patch

from ingestion.destinations.databricks import audit


class IngestionAuditTests(unittest.TestCase):
    def test_write_ingestion_event_maps_manifest_and_source_details(self) -> None:
        manifest = {
            "url": "https://example.test/data.csv",
            "file_size": 123,
            "sha256": "abc123",
        }

        with patch.object(audit, "write_event") as write_event:
            audit.write_ingestion_event(
                "run-1",
                "dataset-1",
                "success",
                source="source-1",
                publication_period="2026-01",
                row_count=42,
                manifest=manifest,
            )

        write_event.assert_called_once()
        args, kwargs = write_event.call_args
        self.assertEqual(args, ("run-1", "dataset-1", "success"))
        self.assertEqual(kwargs["source"], "source-1")
        self.assertEqual(kwargs["source_url"], manifest["url"])
        self.assertEqual(kwargs["publication_period"], "2026-01")
        self.assertEqual(kwargs["row_count"], 42)
        self.assertEqual(kwargs["file_size_bytes"], 123)
        self.assertEqual(kwargs["sha256"], "abc123")
        self.assertIsNotNone(kwargs["completed_at"])

    def test_write_ingestion_event_logs_when_databricks_is_unavailable(self) -> None:
        with (
            patch.object(audit, "write_event", side_effect=RuntimeError("offline")),
            patch("builtins.print") as print_message,
        ):
            audit.write_ingestion_event(
                "run-1",
                "dataset-1",
                "success",
                source="source-1",
            )

        print_message.assert_called_once_with(
            "Databricks audit unavailable; local log retained: offline"
        )

    def test_write_ingestion_event_reraises_unexpected_errors(self) -> None:
        with patch.object(audit, "write_event", side_effect=ValueError("invalid")):
            with self.assertRaisesRegex(ValueError, "invalid"):
                audit.write_ingestion_event(
                    "run-1",
                    "dataset-1",
                    "success",
                    source="source-1",
                )


if __name__ == "__main__":
    unittest.main()
