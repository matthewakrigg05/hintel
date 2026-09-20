import argparse
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from ingestion.common.metadata import add_ingestion_metadata
from ingestion.common.storage import prepare_dataset_dir
from ingestion.destinations.databricks.audit import new_run_id, write_event
from ingestion.destinations.databricks.databricks import RAW_CATALOG, RAW_SCHEMA, bronze_write

from .config import LAND_REGISTRY_DATASETS
from .downloads import (
    append_run_log,
    discover_latest_url,
    get_dataset_manifest,
    is_download_current,
    save_downloaded_dataset,
)


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
        already_retrieved = is_download_current(dataset_id, source_url)
        raw_path = save_downloaded_dataset(dataset_id, source_url, period=period)
    else:
        already_retrieved = False
        raw_path = existing_raw_path(dataset_id)
    frame = load_dataset_from_path(raw_path)
    manifest = get_dataset_manifest(dataset_id) or {}
    result = add_ingestion_metadata(
        frame,
        {
            "_source_dataset": dataset_id,
            "_source_file": str(raw_path),
            "_source_sha256": manifest.get("sha256"),
            "_publication_period": manifest.get("period") if not download else period,
        },
    )
    result.attrs["hpi_period"] = period if download else None
    result.attrs["hpi_download_skipped"] = already_retrieved
    result.attrs["hpi_manifest"] = manifest
    return result


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
    run_id = new_run_id()
    for dataset in LAND_REGISTRY_DATASETS:
        dataset_id = dataset["dataset_id"]
        print(f"Processing {dataset_id}...")
        try:
            frame = ingest_dataset(dataset, download=download)
            results[dataset_id] = frame
            print(f"Loaded {len(frame):,} rows and {len(frame.columns):,} columns for {dataset_id}")
            status = "skipped" if frame.attrs.get("hpi_download_skipped", False) else "success"
            manifest = frame.attrs.get("hpi_manifest", {})
            write_result = bronze_write(
                frame,
                catalog=RAW_CATALOG,
                schema=RAW_SCHEMA,
                table=dataset_id,
                key_cols=["_source_sha256"],
            )
            _write_audit_event(
                run_id,
                dataset_id,
                status,
                period=frame.attrs.get("hpi_period"),
                row_count=write_result["row_count"],
                source_url=manifest.get("url"),
                file_size_bytes=manifest.get("file_size"),
                sha256=manifest.get("sha256"),
            )
            append_run_log(
                dataset_id,
                status,
                run_id=run_id,
                period=frame.attrs.get("hpi_period"),
                row_count=len(frame),
            )
            print(f"Completed {dataset_id}: {status}")
        except Exception as error:
            _write_audit_event(run_id, dataset_id, "failed", error=str(error))
            append_run_log(dataset_id, "failed", run_id=run_id, error=str(error))
            raise
    return results


def _write_audit_event(run_id: str, dataset_id: str, status: str, **details: object) -> None:
    """Write to Databricks when configured, retaining local logging as fallback."""
    try:
        write_event(
            run_id,
            dataset_id,
            status,
            publication_period=details.get("period"),
            source_url=details.get("source_url"),
            row_count=details.get("row_count"),
            file_size_bytes=details.get("file_size_bytes"),
            sha256=details.get("sha256"),
            completed_at=datetime.now(timezone.utc),
            error_message=details.get("error"),
        )
    except (ImportError, RuntimeError, OSError) as error:
        print(f"Databricks audit unavailable; local log retained: {error}")


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