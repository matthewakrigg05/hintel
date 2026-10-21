import argparse
import os
import re
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pandas as pd

from ingestion.common.metadata import add_ingestion_metadata
from ingestion.destinations.databricks.audit import new_run_id, write_event
from ingestion.destinations.databricks.databricks import RAW_CATALOG, bronze_write

if __package__ in {None, ""}:
    from ingestion.epc.config import EPC_BATCH_ROWS, EPC_DATASET_ID, EPC_SCHEMA
    from ingestion.epc.downloads import (
        append_run_log,
        existing_raw_path,
        get_dataset_manifest,
        get_full_load_info,
        is_download_current,
        save_downloaded_dataset,
        update_dataset_manifest,
    )
else:
    from .config import EPC_BATCH_ROWS, EPC_DATASET_ID, EPC_SCHEMA
    from .downloads import (
        append_run_log,
        existing_raw_path,
        get_dataset_manifest,
        get_full_load_info,
        is_download_current,
        save_downloaded_dataset,
        update_dataset_manifest,
    )


EPC_VOLUME_PATH = os.getenv(
    "DATABRICKS_EPC_RAW_VOLUME_PATH",
    f"/Volumes/{RAW_CATALOG}/{EPC_SCHEMA}/files",
)
EPC_FORMATS = {
    "certificates": "certificates",
    "recommendations": "recommendations",
}


def _publication_period(member: str) -> str | None:
    match = re.search(r"(?:19|20)\d{2}", Path(member).stem)
    return match.group(0) if match else None


def ingest_archive(raw_path: Path, manifest: dict) -> dict[str, int]:
    checksum = manifest.get("sha256")
    if not checksum:
        raise ValueError("EPC archive manifest is missing its checksum")

    row_counts = {dataset: 0 for dataset in EPC_FORMATS}
    batch_number = 0
    with zipfile.ZipFile(raw_path) as archive:
        csv_members = [
            member
            for member in archive.infolist()
            if not member.is_dir() and member.filename.lower().endswith(".csv")
        ]
        if not csv_members:
            raise ValueError("EPC archive contains no CSV files")

        members_by_dataset = {
            dataset: [
                member
                for member in csv_members
                if Path(member.filename).name.casefold().startswith(dataset)
            ]
            for dataset in EPC_FORMATS
        }
        missing_datasets = [
            dataset for dataset, members in members_by_dataset.items() if not members
        ]
        if missing_datasets:
            raise ValueError(
                "EPC archive is missing CSV files for: "
                + ", ".join(missing_datasets)
            )

        for member_number, member in enumerate(csv_members, start=1):
            dataset = next(
                (
                    name
                    for name in EPC_FORMATS
                    if Path(member.filename).name.casefold().startswith(name)
                ),
                None,
            )
            if dataset is None:
                print(f"Skipping unrecognized EPC CSV member: {member.filename}")
                continue

            period = _publication_period(member.filename)
            dataset_id = f"{EPC_DATASET_ID}_{dataset}"
            print(f"Processing EPC archive member {member_number}/{len(csv_members)}: {member.filename}")
            with archive.open(member) as source:
                chunks = pd.read_csv(
                    source,
                    dtype=str,
                    encoding="utf-8-sig",
                    chunksize=EPC_BATCH_ROWS,
                    low_memory=False,
                )
                for frame in chunks:
                    batch_number += 1
                    prepared = add_ingestion_metadata(
                        frame,
                        {
                            "_source_dataset": dataset_id,
                            "_source_file": f"{raw_path}!{member.filename}",
                            "_source_sha256": checksum,
                            "_publication_period": period,
                        },
                    )
                    result = bronze_write(
                        prepared,
                        catalog=RAW_CATALOG,
                        schema=EPC_SCHEMA,
                        table=EPC_FORMATS[dataset],
                        key_cols=["_source_sha256"],
                        batch_id=f"{member_number}-{batch_number}",
                        volume_path=EPC_VOLUME_PATH,
                    )
                    row_counts[dataset] += result["row_count"]
                    print(
                        f"Wrote EPC batch {batch_number}: "
                        f"{result['row_count']:,} rows "
                        f"({dataset}: {row_counts[dataset]:,} total)",
                        flush=True,
                    )

    if not any(row_counts.values()):
        raise ValueError("EPC archive contained no certificate or recommendation rows")
    update_dataset_manifest(row_count=sum(row_counts.values()), row_counts=row_counts)
    return row_counts


def run(download: bool = True) -> dict[str, object]:
    run_id = new_run_id()
    raw_path: Path | None = None
    manifest: dict = {}
    already_retrieved = False
    try:
        if download:
            info = get_full_load_info()
            already_retrieved = is_download_current(info["lastUpdated"])
            raw_path = save_downloaded_dataset(info)
            manifest = get_dataset_manifest() or {}
        else:
            raw_path = existing_raw_path()
            manifest = get_dataset_manifest() or {}

        if raw_path is None:
            raise RuntimeError("EPC raw archive was not resolved")
        print(f"Ingesting {EPC_DATASET_ID} from {raw_path}")
        row_counts = ingest_archive(raw_path, manifest)
        row_count = sum(row_counts.values())
        manifest = get_dataset_manifest() or manifest
        status = "skipped" if already_retrieved else "success"
        for dataset, dataset_row_count in row_counts.items():
            _write_audit_event(
                run_id,
                f"{EPC_DATASET_ID}_{dataset}",
                status,
                row_count=dataset_row_count,
                manifest=manifest,
            )
        append_run_log(
            status,
            run_id=run_id,
            last_updated=manifest.get("last_updated"),
            row_count=row_count,
            row_counts=row_counts,
            sha256=manifest.get("sha256"),
        )
        print(f"Completed {EPC_DATASET_ID}: {status}; {row_count:,} rows")
        return {
            "dataset_id": EPC_DATASET_ID,
            "status": status,
            "row_count": row_count,
            "row_counts": row_counts,
        }
    except Exception as error:
        append_run_log("failed", run_id=run_id, error=str(error))
        _write_audit_event(
            run_id,
            EPC_DATASET_ID,
            "failed",
            manifest=manifest,
            error=str(error),
        )
        raise


def _write_audit_event(
    run_id: str,
    dataset_id: str,
    status: str,
    *,
    row_count: int | None = None,
    manifest: dict | None = None,
    error: str | None = None,
) -> None:
    manifest = manifest or {}
    try:
        write_event(
            run_id,
            dataset_id,
            status,
            source="epc_domestic",
            source_url=manifest.get("url"),
            publication_period=manifest.get("last_updated"),
            row_count=row_count,
            file_size_bytes=manifest.get("file_size"),
            sha256=manifest.get("sha256"),
            completed_at=datetime.now(timezone.utc),
            error_message=error,
        )
    except (ImportError, RuntimeError, OSError) as audit_error:
        print(f"Databricks audit unavailable; local log retained: {audit_error}")
    except Exception as audit_error:
        try:
            from databricks.sql.exc import Error as DatabricksError
        except ImportError:
            raise audit_error
        if not isinstance(audit_error, DatabricksError):
            raise
        print(f"Databricks audit unavailable; local log retained: {audit_error}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest domestic EPC certificates")
    parser.add_argument(
        "--no-download",
        action="store_true",
        help="Process the accepted raw EPC archive without checking the API",
    )
    args = parser.parse_args()
    run(download=not args.no_download)


if __name__ == "__main__":
    main()