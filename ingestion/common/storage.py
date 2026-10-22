from pathlib import Path
import os

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