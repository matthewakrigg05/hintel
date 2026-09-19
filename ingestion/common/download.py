
import hashlib
from pathlib import Path

import requests

from .http import get_json


def download_file(url: str, dest: str | Path, headers: dict | None = None, timeout: int = 120) -> Path:
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
    return get_json(url, headers=headers)


def calculate_sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()