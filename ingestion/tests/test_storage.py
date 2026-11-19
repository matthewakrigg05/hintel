import tempfile
import unittest
from pathlib import Path

from ingestion.common.storage import (
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


if __name__ == "__main__":
    unittest.main()
