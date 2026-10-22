
import hashlib
from pathlib import Path

import requests

from .http import get_json


def download_file(url: str, dest: str | Path, headers: dict | None = None, timeout: int = 120) -> Path:
    """
    Download a URL to a local file using an atomic temporary file. Streaming
    avoids holding large datasets in memory, while the temporary file prevents
    an interrupted download from looking complete.

    Args:
        url: URL of the file to download.
        dest: Destination path for the completed file.
        headers: Optional HTTP request headers.
        timeout: Request timeout in seconds.

    Returns:
        The destination path of the downloaded file.
    """
    destination = Path(dest)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(destination.suffix + ".part")
    response = requests.get(url, headers=headers, timeout=timeout, stream=True)
    response.raise_for_status()
    with temporary.open("wb") as output:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            if chunk:
                output.write(chunk)
    temporary.replace(destination)
    return destination


def request_json(url: str, headers: dict | None = None) -> dict:
    """
    Fetch and decode a JSON response so metadata clients share consistent error
    handling and request behavior.

    Args:
        url: URL returning a JSON response.
        headers: Optional HTTP request headers.

    Returns:
        The decoded JSON object.
    """
    return get_json(url, headers=headers)


def calculate_sha256(path: str | Path) -> str:
    """
    Calculate the SHA-256 digest of a local file. A content hash provides a
    stable file identity for detecting duplicate downloads, changed provider
    files, and incomplete or corrupted files.

    Args:
        path: Path to the file to hash.

    Returns:
        The hexadecimal SHA-256 digest.
    """
    digest = hashlib.sha256()
    with Path(path).open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()