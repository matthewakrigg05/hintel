import argparse
from pathlib import Path

import pandas as pd

from ingestion.common.metadata import add_ingestion_metadata
from ingestion.common.storage import prepare_dataset_dir

from .config import LAND_REGISTRY_DATASETS
from .downloads import save_downloaded_dataset


def load_dataset_from_path(file_path: Path) -> pd.DataFrame:
    suffix = file_path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(file_path, low_memory=False)
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(file_path)
    if suffix == ".json":
        return pd.read_json(file_path)
    raise ValueError(f"Unsupported file type: {file_path}")


def existing_raw_path(dataset_id: str) -> Path:
    dataset_dir = prepare_dataset_dir(dataset_id, "land_registry", "hpi")
    matches = sorted(dataset_dir.glob("raw.*"))
    if not matches:
        raise FileNotFoundError(f"No raw file found in {dataset_dir}")
    return matches[0]


def ingest_dataset(dataset: dict, download: bool = True) -> pd.DataFrame:
    dataset_id = dataset["dataset_id"]
    raw_path = save_downloaded_dataset(dataset_id, dataset["url"]) if download else existing_raw_path(dataset_id)
    frame = load_dataset_from_path(raw_path)
    return add_ingestion_metadata(frame, {"_source_dataset": dataset_id, "_source_file": str(raw_path)})


def run_land_registry_all(download: bool = True) -> dict[str, pd.DataFrame]:
    results = {}
    for dataset in LAND_REGISTRY_DATASETS:
        dataset_id = dataset["dataset_id"]
        print(f"Processing {dataset_id}...")
        frame = ingest_dataset(dataset, download=download)
        results[dataset_id] = frame
        print(f"Loaded {len(frame):,} rows and {len(frame.columns):,} columns")
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest Land Registry HPI datasets")
    parser.add_argument("--no-download", action="store_true", help="Process raw files already stored under DATA_PATH")
    args = parser.parse_args()
    run_land_registry_all(download=not args.no_download)


if __name__ == "__main__":
    main()