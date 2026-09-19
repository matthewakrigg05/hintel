from pathlib import Path
import os

from dotenv import load_dotenv

load_dotenv()


def data_root() -> Path:
    configured_path = os.getenv("DATA_PATH")
    if not configured_path:
        raise RuntimeError("DATA_PATH is not configured")
    return Path(configured_path.strip().strip('"').strip("'"))


def prepare_dataset_dir(dataset_id: str, *parts: str) -> Path:
    dataset_dir = data_root().joinpath(*parts, dataset_id)
    dataset_dir.mkdir(parents=True, exist_ok=True)
    return dataset_dir


def write_raw_file(content: bytes, destination: str | Path) -> Path:
    path = Path(destination)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return path