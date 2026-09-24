import argparse
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from ingestion.common.metadata import add_ingestion_metadata
from ingestion.common.storage import prepare_dataset_dir
from ingestion.destinations.databricks.audit import new_run_id, write_event
from ingestion.destinations.databricks.databricks import RAW_CATALOG, RAW_SCHEMA, bronze_write

from .config import (
    PRICE_PAID_BATCH_ROWS,
    PRICE_PAID_COLUMNS,
    PRICE_PAID_DATASET_ID,
    PRICE_PAID_END_YEAR,
)
from .downloads import (
    append_run_log,
    candidate_years,
    discover_url,
    get_dataset_manifest,
    is_download_current,
    save_downloaded_dataset,
    source_url_for_year,
)


def existing_raw_path(year: int) -> Path:
    dataset_dir = prepare_dataset_dir(PRICE_PAID_DATASET_ID, "land_registry", "price_paid")
    raw_path = dataset_dir / f"raw_{year}.csv"
    if not raw_path.exists():
        raise FileNotFoundError(f"No raw file found in {dataset_dir}")
    return raw_path


def load_dataset_from_path(file_path: Path) -> pd.DataFrame:
    return pd.read_csv(
        file_path,
        header=None,
        names=PRICE_PAID_COLUMNS,
        dtype=str,
        low_memory=False,
    )


def ingest_dataset(year: int, download: bool = True) -> pd.DataFrame:
    previous_frame = None
    if download:
        source_url = source_url_for_year(year)
        refresh_current_year = year == PRICE_PAID_END_YEAR
        already_retrieved = is_download_current(year, source_url) and not refresh_current_year
        if already_retrieved:
            print(f"Already retrieved {PRICE_PAID_DATASET_ID} for {year}")
            raw_path = existing_raw_path(year)
        else:
            if refresh_current_year:
                try:
                    previous_frame = load_dataset_from_path(existing_raw_path(year))
                except FileNotFoundError:
                    previous_frame = None
                print(f"Refreshing {PRICE_PAID_DATASET_ID} for {year} to find new rows")
            source_url = discover_url(year)
            raw_path = save_downloaded_dataset(year, source_url, refresh=refresh_current_year)
    else:
        already_retrieved = False
        raw_path = existing_raw_path(year)
        print(f"Using existing raw file for {PRICE_PAID_DATASET_ID} {year}: {raw_path}")

    frame = load_dataset_from_path(raw_path)
    if previous_frame is not None:
        previous_ids = set(previous_frame["transaction_id"].dropna())
        frame = frame[~frame["transaction_id"].isin(previous_ids)].copy()
        print(f"Found {len(frame):,} new rows for {PRICE_PAID_DATASET_ID} {year}")
    manifest = get_dataset_manifest(year) or {}
    result = add_ingestion_metadata(
        frame,
        {
            "_source_dataset": PRICE_PAID_DATASET_ID,
            "_source_file": str(raw_path),
            "_source_sha256": manifest.get("sha256"),
            "_publication_period": manifest.get("period", str(year)),
        },
    )
    result.attrs["price_paid_period"] = manifest.get("period", str(year))
    result.attrs["price_paid_year"] = year
    result.attrs["price_paid_download_skipped"] = already_retrieved
    result.attrs["price_paid_new_rows"] = len(result)
    result.attrs["price_paid_manifest"] = manifest
    return result


def run(download: bool = True) -> dict[int, pd.DataFrame]:
    run_id = new_run_id()
    results = {}
    for year in candidate_years():
        print(f"Processing {PRICE_PAID_DATASET_ID} {year}...")
        try:
            frame = ingest_dataset(year, download=download)
            status = (
                "skipped"
                if frame.attrs.get("price_paid_download_skipped") or not len(frame)
                else "success"
            )
            manifest = frame.attrs.get("price_paid_manifest", {})
            print(
                f"Loaded {len(frame):,} rows and {len(frame.columns):,} columns "
                f"for {PRICE_PAID_DATASET_ID} {year}"
            )
            write_result = {"row_count": 0}
            batch_count = max(1, (len(frame) + PRICE_PAID_BATCH_ROWS - 1) // PRICE_PAID_BATCH_ROWS)
            for batch_number, start in enumerate(
                range(0, len(frame), PRICE_PAID_BATCH_ROWS),
                start=1,
            ):
                batch = frame.iloc[start : start + PRICE_PAID_BATCH_ROWS]
                print(
                    f"Writing batch {batch_number}/{batch_count} for "
                    f"{PRICE_PAID_DATASET_ID} {year}: {len(batch):,} rows",
                    flush=True,
                )
                batch_result = bronze_write(
                    batch,
                    catalog=RAW_CATALOG,
                    schema=RAW_SCHEMA,
                    table=PRICE_PAID_DATASET_ID,
                    key_cols=["_source_sha256"],
                    batch_id=f"{year}-{batch_number}",
                )
                write_result["row_count"] += batch_result["row_count"]
            _write_audit_event(
                run_id,
                status,
                period=frame.attrs.get("price_paid_period"),
                row_count=write_result["row_count"],
                source_url=manifest.get("url"),
                file_size_bytes=manifest.get("file_size"),
                sha256=manifest.get("sha256"),
            )
            append_run_log(run_id=run_id, status=status, period=str(year), row_count=len(frame))
            print(f"Completed {PRICE_PAID_DATASET_ID} {year}: {status}")
            results[year] = frame
        except Exception as error:
            _write_audit_event(run_id, "failed", period=str(year), error=str(error))
            append_run_log(run_id=run_id, status="failed", period=str(year), error=str(error))
            raise
    return results


def _write_audit_event(run_id: str, status: str, **details: object) -> None:
    try:
        write_event(
            run_id,
            PRICE_PAID_DATASET_ID,
            status,
            source="land_registry_price_paid",
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
    parser = argparse.ArgumentParser(description="Ingest HM Land Registry Price Paid data")
    parser.add_argument("--no-download", action="store_true", help="Process annual raw files already stored under DATA_PATH")
    args = parser.parse_args()
    run(download=not args.no_download)


if __name__ == "__main__":
    main()