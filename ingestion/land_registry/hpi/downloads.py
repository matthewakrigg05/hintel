from datetime import date
from pathlib import Path
from urllib.parse import urlparse

import requests

from ingestion.common.download import download_file
from ingestion.common.http import get_json
from ingestion.common.storage import prepare_dataset_dir

from .config import LAND_REGISTRY_API_KEY


HPI_BASE_URL = (
    "https://publicdata.landregistry.gov.uk/"
    "market-trend-data/house-price-index-data"
)


def candidate_periods(lookback: int = 24):
    """Yield HPI periods from the current month backwards."""
    current_month = date.today().year * 12 + date.today().month - 1
    for offset in range(lookback):
        month_index = current_month - offset
        year, month = divmod(month_index, 12)
        yield f"{year:04d}-{month + 1:02d}"


def discover_latest_url(filename: str, lookback: int = 24) -> tuple[str, str]:
    """Find the newest published CSV for an HPI filename stem."""
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


def save_downloaded_dataset(dataset_id: str, source_url: str) -> Path:
    """
    Resolve and save one HPI dataset in the configured raw-data area. This
    combines HPI URL resolution with the shared downloader while keeping
    provider-specific path conventions in one place.

    Args:
        dataset_id: Identifier used for the dataset directory.
        source_url: Direct file or metadata URL for the dataset.

    Returns:
        The path of the saved raw dataset file.
    """
    headers = auth_headers()
    download_url, file_type = resolve_download_url(source_url, headers=headers)
    destination = prepare_dataset_dir(dataset_id, "land_registry", "hpi") / f"raw.{file_type}"
    return download_file(download_url, destination, headers=headers)