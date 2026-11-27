import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from ingestion.land_registry.common import download_and_accept


class DownloadAcceptanceTests(unittest.TestCase):
    def test_download_and_accept_writes_file_and_manifest(self) -> None:
        content = b"transaction_id,price\n1,100\n2,200\n"
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            destination = directory / "raw.csv"
            manifest_path = directory / "manifest.json"

            def write_download(_url: str, path: Path, **_kwargs: object) -> Path:
                path.write_bytes(content)
                return path

            with patch(
                "ingestion.land_registry.common.download_file",
                side_effect=write_download,
            ):
                accepted_path, details = download_and_accept(
                    "https://example.test/data.csv",
                    destination,
                    manifest_path=manifest_path,
                    details={
                        "dataset_id": "test",
                        "url": "https://example.test/data.csv",
                    },
                    previous_manifest=None,
                    source_name="test",
                    inspect_file=lambda _path: (["transaction_id", "price"], 2),
                )

            self.assertEqual(accepted_path, destination)
            self.assertEqual(destination.read_bytes(), content)
            self.assertEqual(details["row_count"], 2)
            self.assertEqual(
                json.loads(manifest_path.read_text(encoding="utf-8")),
                details,
            )
            self.assertFalse(destination.with_suffix(".csv.candidate").exists())

    def test_rejected_download_keeps_accepted_file_and_cleans_candidate(self) -> None:
        accepted_content = b"accepted"
        candidate_content = b"larger candidate"
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            destination = directory / "raw.csv"
            destination.write_bytes(accepted_content)
            manifest_path = directory / "manifest.json"

            def write_download(_url: str, path: Path, **_kwargs: object) -> Path:
                path.write_bytes(candidate_content)
                return path

            with (
                patch(
                    "ingestion.land_registry.common.download_file",
                    side_effect=write_download,
                ),
                self.assertRaisesRegex(ValueError, "fewer rows"),
            ):
                download_and_accept(
                    "https://example.test/data.csv",
                    destination,
                    manifest_path=manifest_path,
                    details={"dataset_id": "test"},
                    previous_manifest={"file_size": 1, "row_count": 3},
                    source_name="test",
                    inspect_file=lambda _path: (["id"], 2),
                )

            self.assertEqual(destination.read_bytes(), accepted_content)
            self.assertFalse(destination.with_suffix(".csv.candidate").exists())
            self.assertFalse(
                destination.with_suffix(".csv.candidate.part").exists()
            )


if __name__ == "__main__":
    unittest.main()
