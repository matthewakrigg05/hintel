from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path

from ingestion.common.download import calculate_sha256, download_file
from ingestion.common.storage import write_manifest


def download_and_accept(
    url: str,
    destination: str | Path,
    *,
    manifest_path: str | Path,
    details: dict,
    previous_manifest: dict | None,
    source_name: str,
    inspect_file: Callable[[Path], tuple[list[str], int]],
    headers: dict | None = None,
    timeout: int = 120,
) -> tuple[Path, dict]:
    """Download, validate, and atomically accept a Land Registry data file."""
    destination = Path(destination)
    manifest_path = Path(manifest_path)
    candidate = destination.with_suffix(destination.suffix + ".candidate")
    candidate_part = candidate.with_suffix(candidate.suffix + ".part")
    candidate.unlink(missing_ok=True)

    try:
        download_file(url, candidate, headers=headers, timeout=timeout)
        columns, row_count = inspect_file(candidate)
        if not columns or row_count == 0:
            raise ValueError(f"Downloaded {source_name} file is empty: {url}")

        file_size = candidate.stat().st_size
        previous_manifest = previous_manifest or {}
        if file_size < previous_manifest.get("file_size", 0):
            raise ValueError(
                f"New {source_name} file is smaller than the accepted file: {url}"
            )
        if row_count < previous_manifest.get("row_count", 0):
            raise ValueError(
                f"New {source_name} file has fewer rows than the accepted file: "
                f"{url}"
            )

        accepted_details = {
            **details,
            "file_size": file_size,
            "row_count": row_count,
            "columns": columns,
            "sha256": calculate_sha256(candidate),
            "accepted_at": datetime.now(timezone.utc).isoformat(),
        }
        candidate.replace(destination)
        write_manifest(
            manifest_path.parent,
            accepted_details,
            manifest_path.name,
        )
        return destination, accepted_details
    except Exception:
        candidate.unlink(missing_ok=True)
        candidate_part.unlink(missing_ok=True)
        raise
