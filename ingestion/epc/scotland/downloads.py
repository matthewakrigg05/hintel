import json
from datetime import datetime, timezone
from pathlib import Path

import py7zr

from ingestion.common.download import calculate_sha256, download_file
from ingestion.common.storage import data_root, prepare_dataset_dir

from .config import SCOTLAND_DATASETS


def _dataset_dir(dataset: str) -> Path:
    if dataset not in SCOTLAND_DATASETS:
        raise ValueError(f"Unsupported Scottish EPC dataset: {dataset}")
    return prepare_dataset_dir(dataset, "epc", "scotland")


def _manifest_path(dataset: str) -> Path:
    return _dataset_dir(dataset) / "manifest.json"


def get_dataset_manifest(dataset: str) -> dict | None:
    path = _manifest_path(dataset)
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _write_manifest(dataset: str, details: dict) -> None:
    path = _manifest_path(dataset)
    temporary = path.with_suffix(".json.part")
    temporary.write_text(json.dumps(details, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def save_downloaded_dataset(dataset: str, refresh: bool = False) -> Path:
    definition = SCOTLAND_DATASETS.get(dataset)
    if definition is None:
        raise ValueError(f"Unsupported Scottish EPC dataset: {dataset}")

    dataset_dir = _dataset_dir(dataset)
    destination = dataset_dir / "raw.7z"
    previous = get_dataset_manifest(dataset)
    if not refresh and previous and destination.exists():
        print(f"Already retrieved Scottish EPC {dataset}")
        return destination

    temporary = dataset_dir / "raw.7z.candidate"
    temporary.unlink(missing_ok=True)
    try:
        print(f"Downloading Scottish EPC {dataset} from {definition['url']}")
        download_file(definition["url"], temporary, timeout=1800)
        with py7zr.SevenZipFile(temporary, mode="r") as archive:
            members = [
                name
                for name in archive.getnames()
                if name.lower().endswith(".csv")
            ]
        if not members:
            raise ValueError(
                f"Scottish EPC {dataset} archive contains no CSV files"
            )

        details = {
            "dataset_id": f"epc_scotland_{dataset}",
            "url": definition["url"],
            "file_size": temporary.stat().st_size,
            "file_type": "7z",
            "members": members,
            "sha256": calculate_sha256(temporary),
            "accepted_at": datetime.now(timezone.utc).isoformat(),
        }
        temporary.replace(destination)
        _write_manifest(dataset, details)
        return destination
    except Exception:
        temporary.unlink(missing_ok=True)
        temporary.with_suffix(temporary.suffix + ".part").unlink(missing_ok=True)
        raise


def existing_raw_path(dataset: str) -> Path:
    raw_path = _dataset_dir(dataset) / "raw.7z"
    if not raw_path.exists():
        raise FileNotFoundError(
            f"No Scottish EPC raw archive found in {raw_path.parent}"
        )
    return raw_path


def update_dataset_manifest(dataset: str, **details: object) -> dict:
    manifest = get_dataset_manifest(dataset) or {}
    manifest.update(details)
    _write_manifest(dataset, manifest)
    return manifest


def append_run_log(status: str, **details: object) -> None:
    log_path = data_root() / "epc" / "scotland" / "run_log.jsonl"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    event = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "status": status,
        **details,
    }
    with log_path.open("a", encoding="utf-8") as log:
        log.write(json.dumps(event) + "\n")
