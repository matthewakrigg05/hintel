from pathlib import Path
from urllib.parse import urlparse

from ingestion.common.download import download_file
from ingestion.common.http import get_json
from ingestion.common.storage import prepare_dataset_dir

from .config import LAND_REGISTRY_API_KEY


def auth_headers() -> dict[str, str]:
    if not LAND_REGISTRY_API_KEY:
        return {}
    return {"Authorization": LAND_REGISTRY_API_KEY}


def get_download_url_from_version(version_url: str, headers: dict | None = None) -> tuple[str, str]:
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
    headers = auth_headers()
    download_url, file_type = resolve_download_url(source_url, headers=headers)
    destination = prepare_dataset_dir(dataset_id, "land_registry", "hpi") / f"raw.{file_type}"
    return download_file(download_url, destination, headers=headers)