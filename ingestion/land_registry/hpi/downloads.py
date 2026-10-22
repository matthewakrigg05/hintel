import csv
import hashlib
import json
from datetime import datetime, timezone
from datetime import date
from pathlib import Path
from urllib.parse import urlparse

import requests

from ingestion.common.download import download_file
from ingestion.common.http import get_json
from ingestion.common.storage import data_root, prepare_dataset_dir

from .config import LAND_REGISTRY_API_KEY


HPI_BASE_URL = (
    "https://publicdata.landregistry.gov.uk/"
    "market-trend-data/house-price-index-data"
)


def _sha256(path: Path) -> str:
    """Calculate the SHA-256 checksum of a local file."""
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _csv_details(path: Path) -> tuple[list[str], int]:
    """Return the header names and data-row count for a CSV file."""
    with path.open("r", encoding="utf-8-sig", newline="") as source:
        reader = csv.reader(source)
        columns = next(reader, [])
        row_count = sum(1 for _ in reader)
    return columns, row_count


def _manifest_path(dataset_dir: Path) -> Path:
    """Return the path of the accepted-file manifest for a dataset."""
    return dataset_dir / "manifest.json"


def _load_manifest(dataset_dir: Path) -> dict | None:
    """Load a dataset manifest, returning ``None`` before first acceptance."""
    path = _manifest_path(dataset_dir)
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def is_download_current(dataset_id: str, source_url: str) -> bool:
    """Return whether the accepted raw CSV already matches a source URL.

    The manifest URL and the stable ``raw.csv`` path must both be present. This
    makes a rerun safe when discovery returns the same published version.
    """
    dataset_dir = prepare_dataset_dir(dataset_id, "land_registry", "hpi")
    manifest = _load_manifest(dataset_dir)
    return bool(
        manifest
        and (dataset_dir / "raw.csv").exists()
        and manifest.get("url") == source_url
    )


def get_dataset_manifest(dataset_id: str) -> dict | None:
    """Return the accepted manifest for a dataset, if one exists."""
    dataset_dir = prepare_dataset_dir(dataset_id, "land_registry", "hpi")
    return _load_manifest(dataset_dir)


def _write_manifest(dataset_dir: Path, details: dict) -> None:
    """Atomically write metadata for the newly accepted raw file."""
    temporary = _manifest_path(dataset_dir).with_suffix(".json.part")
    temporary.write_text(json.dumps(details, indent=2) + "\n", encoding="utf-8")
    temporary.replace(_manifest_path(dataset_dir))


def append_run_log(dataset_id: str, status: str, **details: object) -> None:
    """Append one ingestion outcome to the shared HPI JSONL run log.

    Args:
        dataset_id: Stable logical identifier for the dataset.
        status: Outcome such as ``success``, ``skipped``, or ``failed``.
        **details: Serializable event fields such as period, row count, or
            an error message.
    """
    log_path = data_root() / "land_registry" / "hpi" / "run_log.jsonl"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    event = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "dataset_id": dataset_id,
        "status": status,
        **details,
    }
    with log_path.open("a", encoding="utf-8") as log:
        log.write(json.dumps(event) + "\n")


def candidate_periods(lookback: int = 24):
    """Yield ``YYYY-MM`` HPI periods from the current month backwards.

    Args:
        lookback: Maximum number of monthly periods to inspect.
    """
    current_month = date.today().year * 12 + date.today().month - 1
    for offset in range(lookback):
        month_index = current_month - offset
        year, month = divmod(month_index, 12)
        yield f"{year:04d}-{month + 1:02d}"


def discover_latest_url(filename: str, lookback: int = 24) -> tuple[str, str]:
    """Find the newest published CSV for an HPI filename stem.

    Args:
        filename: Land Registry filename stem without period or extension.
        lookback: Maximum number of periods to probe.

    Returns:
        A tuple containing the available URL and its ``YYYY-MM`` period.

    Raises:
        RuntimeError: If no file is available within the lookback window.
    """
    for period in candidate_periods(lookback):
        url = f"{HPI_BASE_URL}/{filename}-{period}.csv"
        try:
            response = requests.head(url, timeout=30, allow_redirects=True)
            if response.status_code == 405:
                response = requests.get(url, timeout=30, stream=True)
            response.close()
        except requests.RequestException:
            continue

        if response.status_code == 200:
            return url, period

    raise RuntimeError(f"No available HPI CSV found for {filename}")


def auth_headers() -> dict[str, str]:
    """
    Build authorization headers for Land Registry requests so authentication
    stays at the source boundary and shared download code remains provider-neutral.

    Returns:
        A headers dictionary containing the configured API key when available.
    """
    if not LAND_REGISTRY_API_KEY:
        return {}
    return {"Authorization": LAND_REGISTRY_API_KEY}


def get_download_url_from_version(version_url: str, headers: dict | None = None) -> tuple[str, str]:
    """
    Select the preferred downloadable file from a version response. Land
    Registry metadata can expose several formats, so the ingestion pipeline needs
    one predictable preference order for raw files.

    Args:
        version_url: URL of a Land Registry version metadata endpoint.
        headers: Optional HTTP request headers.

    Returns:
        A tuple containing the download URL and lowercase file type.

    Raises:
        ValueError: If the version contains no usable download link.
    """
    version_data = get_json(version_url, headers=headers)
    downloads = version_data.get("downloads", {})
    for key in ("csv", "xlsx", "xls", "json"):
        entry = downloads.get(key)
        href = entry.get("href") or entry.get("url") if isinstance(entry, dict) else entry
        if href:
            return href, key
    for key, value in downloads.items():
        if isinstance(value, dict):
            href = value.get("href") or value.get("url")
            if href:
                return href, key
    raise ValueError(f"No downloadable file found for version: {version_url}")


def resolve_download_url(source_url: str, headers: dict | None = None) -> tuple[str, str]:
    """
    Resolve a direct file URL or API metadata URL to a downloadable file so
    source configuration can use stable provider URLs without duplicating flow.

    Args:
        source_url: Direct file URL or Land Registry metadata URL.
        headers: Optional HTTP request headers.

    Returns:
        A tuple containing the download URL and lowercase file type.

    Raises:
        ValueError: If the metadata does not expose a downloadable file.
    """
    suffix = Path(urlparse(source_url).path).suffix.lower().lstrip(".")
    if suffix in {"csv", "xlsx", "xls", "json"}:
        return source_url, suffix
    metadata = get_json(source_url, headers=headers)
    latest = metadata.get("links", {}).get("latest_version")
    if isinstance(latest, dict) and latest.get("href"):
        return get_download_url_from_version(latest["href"], headers=headers)
    if metadata.get("downloads"):
        return get_download_url_from_version(source_url, headers=headers)
    raise ValueError(f"Could not resolve a download URL from: {source_url}")


def save_downloaded_dataset(dataset_id: str, source_url: str, period: str | None = None) -> Path:
    """
    Resolve, validate, and save one HPI dataset in the configured raw-data area.
    The accepted file is recorded in a manifest, and an unchanged discovered
    URL is skipped so repeated monthly runs are idempotent.

    Args:
        dataset_id: Identifier used for the dataset directory.
        source_url: Direct file or metadata URL for the dataset.
        period: Publication period discovered for the source URL.

    Returns:
        The path of the saved raw dataset file.
    """
    headers = auth_headers()
    download_url, file_type = resolve_download_url(source_url, headers=headers)
    dataset_dir = prepare_dataset_dir(dataset_id, "land_registry", "hpi")
    destination = dataset_dir / f"raw.{file_type}"
    previous = _load_manifest(dataset_dir)
    if previous and destination.exists() and previous.get("url") == download_url:
        print(f"Already retrieved {dataset_id} for {previous.get('period', download_url)}")
        return destination

    temporary = destination.with_suffix(destination.suffix + ".candidate")

    download_file(download_url, temporary, headers=headers)
    columns, row_count = _csv_details(temporary)
    previous_size = previous.get("file_size", 0) if previous else 0
    previous_rows = previous.get("row_count", 0) if previous else 0

    if not columns or row_count == 0:
        temporary.unlink(missing_ok=True)
        raise ValueError(f"Downloaded HPI file is empty: {download_url}")
    if previous and temporary.stat().st_size < previous_size:
        temporary.unlink(missing_ok=True)
        raise ValueError(f"New HPI file is smaller than the accepted file: {download_url}")
    if previous and row_count < previous_rows:
        temporary.unlink(missing_ok=True)
        raise ValueError(f"New HPI file has fewer rows than the accepted file: {download_url}")

    details = {
        "dataset_id": dataset_id,
        "url": download_url,
        "period": period,
        "file_type": file_type,
        "file_size": temporary.stat().st_size,
        "row_count": row_count,
        "columns": columns,
        "sha256": _sha256(temporary),
        "accepted_at": datetime.now(timezone.utc).isoformat(),
    }
    temporary.replace(destination)
    _write_manifest(dataset_dir, details)
    return destination