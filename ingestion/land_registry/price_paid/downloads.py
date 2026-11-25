import csv
from pathlib import Path

import requests

from ingestion.common.storage import (
    append_jsonl_event,
    load_manifest,
    prepare_dataset_dir,
)
from ingestion.land_registry.common import download_and_accept

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

    print(f"Downloading {PRICE_PAID_DATASET_ID} for {year} from {source_url}")
    _, accepted_details = download_and_accept(
        source_url,
        destination,
        manifest_path=dataset_dir / f"manifest_{year}.json",
        details={
            "dataset_id": PRICE_PAID_DATASET_ID,
            "url": source_url,
            "period": str(year),
            "file_type": "csv",
        },
        previous_manifest=previous,
        source_name="Price Paid",
        inspect_file=_csv_details,
    )
    print(
        f"Validated {accepted_details['row_count']:,} rows "
        f"for {PRICE_PAID_DATASET_ID}"
    )
    return destination