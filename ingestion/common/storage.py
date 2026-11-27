import json
import os
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


def data_root() -> Path:
    """
    Resolve the configured root directory for landed data. Reading the root from
    configuration keeps data outside the repository and allows the same
    ingestion code to run in different environments.

    Returns:
        The normalized path configured by the ``DATA_PATH`` environment variable.

    Raises:
        RuntimeError: If ``DATA_PATH`` is not configured.
    """
    configured_path = os.getenv("DATA_PATH")
    if not configured_path:
        raise RuntimeError("DATA_PATH is not configured")
    return Path(configured_path.strip().strip('"').strip("'"))


def prepare_dataset_dir(dataset_id: str, *parts: str) -> Path:
    """
    Create and return a dataset directory below the data root. A predictable
    source-specific directory gives raw files a stable place to land and makes
    reruns and downstream discovery straightforward.

    Args:
        dataset_id: Identifier used for the final directory name.
        *parts: Intermediate directory names below the data root.

    Returns:
        The created dataset directory path.
    """
    dataset_dir = data_root().joinpath(*parts, dataset_id)
    dataset_dir.mkdir(parents=True, exist_ok=True)
    return dataset_dir


def write_raw_file(content: bytes, destination: str | Path) -> Path:
    """
    Write raw file content to a local destination. This separates filesystem
    persistence from HTTP and source-specific logic, so other ingestion sources
    can reuse the same storage behavior.

    Args:
        content: File content as bytes.
        destination: Destination path for the raw file.

    Returns:
        The destination path of the written file.
    """
    path = Path(destination)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return path


def manifest_path(dataset_dir: Path, filename: str = "manifest.json") -> Path:
    """Return the path of the accepted-file manifest for a dataset."""
    return dataset_dir / filename


def load_manifest(
    dataset_dir: Path,
    filename: str = "manifest.json",
) -> dict | None:
    """Load a dataset manifest, returning ``None`` before first acceptance."""
    path = manifest_path(dataset_dir, filename)
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def write_manifest(
    dataset_dir: Path,
    details: dict,
    filename: str = "manifest.json",
) -> Path:
    """Atomically write a dataset manifest and return its path."""
    path = manifest_path(dataset_dir, filename)
    temporary = path.with_suffix(path.suffix + ".part")
    temporary.write_text(json.dumps(details, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)
    return path


def update_manifest(
    dataset_dir: Path,
    details: dict,
    filename: str = "manifest.json",
) -> dict:
    """Merge fields into a dataset manifest, write it, and return the result."""
    manifest = load_manifest(dataset_dir, filename) or {}
    manifest.update(details)
    write_manifest(dataset_dir, manifest, filename)
    return manifest


def append_jsonl_event(parts: tuple[str, ...], event: dict[str, object]) -> None:
    """Append a timestamped event to a JSONL file below the data root."""
    log_path = data_root().joinpath(*parts, "run_log.jsonl")
    log_path.parent.mkdir(parents=True, exist_ok=True)
    record = {"timestamp": datetime.now(timezone.utc).isoformat(), **event}
    with log_path.open("a", encoding="utf-8") as log:
        log.write(json.dumps(record) + "\n")
