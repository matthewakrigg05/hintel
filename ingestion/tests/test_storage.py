import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from ingestion.common.storage import (
    append_jsonl_event,
    load_manifest,
    manifest_path,
    update_manifest,
    write_manifest,
)


class ManifestStorageTests(unittest.TestCase):
    def test_manifest_helpers_support_named_files_and_updates(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            dataset_dir = Path(temporary)
            filename = "manifest_2025.json"

            self.assertIsNone(load_manifest(dataset_dir, filename))
            self.assertEqual(
                manifest_path(dataset_dir, filename),
                dataset_dir / filename,
            )

            write_manifest(dataset_dir, {"year": 2025, "row_count": 10}, filename)
            updated = update_manifest(dataset_dir, {"row_count": 12}, filename)

            self.assertEqual(updated, {"year": 2025, "row_count": 12})
            self.assertEqual(load_manifest(dataset_dir, filename), updated)
            self.assertFalse((dataset_dir / f"{filename}.part").exists())


class JsonlStorageTests(unittest.TestCase):
    def test_append_jsonl_event_creates_timestamped_log_under_data_root(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            with patch.dict(os.environ, {"DATA_PATH": temporary}):
                append_jsonl_event(
                    ("epc", "england_wales"),
                    {"dataset_id": "epc_domestic", "status": "success"},
                )
                append_jsonl_event(
                    ("epc", "england_wales"),
                    {"dataset_id": "epc_non_domestic", "status": "failed"},
                )

            log_path = (
                Path(temporary)
                / "epc"
                / "england_wales"
                / "run_log.jsonl"
            )
            events = [
                json.loads(line)
                for line in log_path.read_text(encoding="utf-8").splitlines()
            ]

            self.assertEqual(len(events), 2)
            self.assertEqual(events[0]["dataset_id"], "epc_domestic")
            self.assertEqual(events[0]["status"], "success")
            self.assertTrue(events[0]["timestamp"])
            self.assertEqual(events[1]["dataset_id"], "epc_non_domestic")
            self.assertEqual(events[1]["status"], "failed")


if __name__ == "__main__":
    unittest.main()
