import csv
from datetime import datetime, timezone
from pathlib import Path

import requests

from ingestion.common.download import download_file, calculate_sha256
from ingestion.common.storage import (
    append_jsonl_event,
    load_manifest,
    prepare_dataset_dir,
    write_manifest,
)

from .config import (
    PRICE_PAID_BASE_URL,
    PRICE_PAID_COLUMNS,
    PRICE_PAID_DATASET_ID,
    PRICE_PAID_END_YEAR,
    PRICE_PAID_START_YEAR,
)


def _csv_details(path: Path) -> tuple[list[str], int]:
    with path.open("r", encoding="utf-8-sig", newline="") as source:
        reader = csv.reader(source)
        row_count = sum(1 for row in reader if row)
    return PRICE_PAID_COLUMNS.copy(), row_count


def _load_manifest(year: int) -> dict | None:
    dataset_dir = prepare_dataset_dir(
        PRICE_PAID_DATASET_ID, "land_registry", "price_paid"
    )
    return load_manifest(dataset_dir, f"manifest_{year}.json")


def get_dataset_manifest(year: int) -> dict | None:
    return _load_manifest(year)


def _write_manifest(year: int, details: dict) -> None:
    dataset_dir = prepare_dataset_dir(
        PRICE_PAID_DATASET_ID, "land_registry", "price_paid"
    )
    write_manifest(dataset_dir, details, f"manifest_{year}.json")


def append_run_log(status: str, **details: object) -> None:
    append_jsonl_event(
        ("land_registry", "price_paid"),
        {"dataset_id": PRICE_PAID_DATASET_ID, "status": status, **details},
    )


def candidate_years():
    return range(PRICE_PAID_START_YEAR, PRICE_PAID_END_YEAR + 1)


def source_url_for_year(year: int) -> str:
    return PRICE_PAID_BASE_URL.replace(str(PRICE_PAID_END_YEAR), str(year))


def discover_url(year: int) -> str:
    url = source_url_for_year(year)
    print(f"Checking Price Paid source for {year}: {url}")
    try:
        response = requests.get(url, timeout=30, stream=True)
        response.close()
    except requests.RequestException as error:
        raise RuntimeError(f"Unable to reach Price Paid source: {url}") from error
    if response.status_code != 200:
        raise RuntimeError(f"Price Paid source returned HTTP {response.status_code}: {url}")
    return url


def is_download_current(year: int, source_url: str) -> bool:
    dataset_dir = prepare_dataset_dir(
        PRICE_PAID_DATASET_ID, "land_registry", "price_paid"
    )
    manifest = _load_manifest(year)
    return bool(
        manifest
        and (dataset_dir / f"raw_{year}.csv").exists()
        and manifest.get("url") == source_url
    )


def save_downloaded_dataset(year: int, source_url: str, refresh: bool = False) -> Path:
    dataset_dir = prepare_dataset_dir(
        PRICE_PAID_DATASET_ID, "land_registry", "price_paid"
    )
    destination = dataset_dir / f"raw_{year}.csv"
    previous = _load_manifest(year)
    if not refresh and previous and destination.exists() and previous.get("url") == source_url:
        print(f"Already retrieved {PRICE_PAID_DATASET_ID} for {year}")
        return destination

    temporary = dataset_dir / "raw.csv.candidate"
    print(f"Downloading {PRICE_PAID_DATASET_ID} for {year} from {source_url}")
    download_file(source_url, temporary)
    columns, row_count = _csv_details(temporary)
    previous_size = previous.get("file_size", 0) if previous else 0
    previous_rows = previous.get("row_count", 0) if previous else 0
    if not columns or row_count == 0:
        temporary.unlink(missing_ok=True)
        raise ValueError(f"Downloaded Price Paid file is empty: {source_url}")
    if previous and temporary.stat().st_size < previous_size:
        temporary.unlink(missing_ok=True)
        raise ValueError(f"New Price Paid file is smaller than the accepted file: {source_url}")
    if previous and row_count < previous_rows:
        temporary.unlink(missing_ok=True)
        raise ValueError(f"New Price Paid file has fewer rows than the accepted file: {source_url}")

    print(f"Validated {row_count:,} rows for {PRICE_PAID_DATASET_ID}")

    details = {
        "dataset_id": PRICE_PAID_DATASET_ID,
        "url": source_url,
        "period": str(year),
        "file_type": "csv",
        "file_size": temporary.stat().st_size,
        "row_count": row_count,
        "columns": columns,
        "sha256": calculate_sha256(temporary),
        "accepted_at": datetime.now(timezone.utc).isoformat(),
    }
    temporary.replace(destination)
    _write_manifest(year, details)
    return destination