import argparse
from pathlib import Path

import pandas as pd

from ingestion.common.metadata import add_ingestion_metadata
from ingestion.common.storage import prepare_dataset_dir

from .config import LAND_REGISTRY_DATASETS
from .downloads import discover_latest_url, save_downloaded_dataset


def load_dataset_from_path(file_path: Path) -> pd.DataFrame:
    """
    Load a supported raw dataset file into a pandas dataframe. Keeping
    file-format handling at the loading boundary means downloading remains
    independent of pandas and source transformation logic.

    Args:
        file_path: Path to a CSV, Excel, or JSON file.

    Returns:
        The loaded pandas dataframe.

    Raises:
        ValueError: If the file extension is unsupported.
    """
    suffix = file_path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(file_path, low_memory=False)
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(file_path)
    if suffix == ".json":
        return pd.read_json(file_path)
    raise ValueError(f"Unsupported file type: {file_path}")


def existing_raw_path(dataset_id: str) -> Path:
    """
    Find the existing raw file for an HPI dataset. No-download runs are
    useful for reprocessing or testing without making another provider request.

    Args:
        dataset_id: Identifier used for the dataset directory.

    Returns:
        The path of the first matching raw file.

    Raises:
        FileNotFoundError: If the dataset has no raw file.
    """
    dataset_dir = prepare_dataset_dir(dataset_id, "land_registry", "hpi")
    matches = sorted(dataset_dir.glob("raw.*"))
    if not matches:
        raise FileNotFoundError(f"No raw file found in {dataset_dir}")
    return matches[0]


def ingest_dataset(dataset: dict, download: bool = True) -> pd.DataFrame:
    """
    Download or load one HPI dataset and attach source metadata. This is the
    repeatable unit of work for one dataset and gives callers a consistent
    dataframe regardless of whether it was freshly downloaded.

    Args:
        dataset: Dataset configuration containing ``dataset_id`` and ``filename``.
        download: Whether to download the source before loading it.

    Returns:
        The loaded dataframe with ingestion metadata columns.
    """
    dataset_id = dataset["dataset_id"]
    if download:
        source_url, period = discover_latest_url(dataset["filename"])
        print(f"Using {period} for {dataset_id}")
        raw_path = save_downloaded_dataset(dataset_id, source_url)
    else:
        raw_path = existing_raw_path(dataset_id)
    frame = load_dataset_from_path(raw_path)
    return add_ingestion_metadata(frame, {"_source_dataset": dataset_id, "_source_file": str(raw_path)})


def run_land_registry_all(download: bool = True) -> dict[str, pd.DataFrame]:
    """
    Ingest all configured Land Registry HPI datasets. A single orchestration
    entry point makes scheduled runs and manual refreshes use the same dataset
    configuration and processing path.

    Args:
        download: Whether to download each source before loading it.

    Returns:
        A mapping from dataset identifier to loaded dataframe.
    """
    results = {}
    for dataset in LAND_REGISTRY_DATASETS:
        dataset_id = dataset["dataset_id"]
        print(f"Processing {dataset_id}...")
        frame = ingest_dataset(dataset, download=download)
        results[dataset_id] = frame
        print(f"Loaded {len(frame):,} rows and {len(frame.columns):,} columns")
    return results


def main() -> None:
    """
    Parse command-line options and run the HPI ingestion pipeline so it can
    be called by a scheduler, CI job, or local developer.

    Returns:
        None.
    """
    parser = argparse.ArgumentParser(description="Ingest Land Registry HPI datasets")
    parser.add_argument("--no-download", action="store_true", help="Process raw files already stored under DATA_PATH")
    args = parser.parse_args()
    run_land_registry_all(download=not args.no_download)


if __name__ == "__main__":
    main()