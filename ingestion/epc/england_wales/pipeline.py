import argparse
import os
import re
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

import pandas as pd

from ingestion.common.metadata import add_ingestion_metadata
from ingestion.destinations.databricks.audit import new_run_id, write_event
from ingestion.destinations.databricks.databricks import RAW_CATALOG, bronze_write

if __package__ in {None, ""}:
    from ingestion.epc.config import EPC_BATCH_ROWS, EPC_SCHEMA
    from ingestion.epc.england_wales.config import (
        EPC_DATASET_ID,
        EPC_DATASET_IDS,
    )
    from ingestion.epc.england_wales.downloads import (
        append_run_log,
        existing_raw_path,
        get_dataset_manifest,
        get_full_load_info,
        is_download_current,
        save_downloaded_dataset,
        update_dataset_manifest,
    )
else:
    from ..config import EPC_BATCH_ROWS, EPC_SCHEMA
    from .config import EPC_DATASET_ID, EPC_DATASET_IDS
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
EPC_NON_DOMESTIC_FORMATS = {"non_domestic": "non_domestic"}


def _publication_period(member: str) -> str | None:
    match = re.search(r"(?:19|20)\d{2}", Path(member).stem)
    return match.group(0) if match else None


def ingest_archive(
    raw_path: Path,
    manifest: dict,
    dataset: str = "domestic",
) -> dict[str, int]:
    checksum = manifest.get("sha256")
    if not checksum:
        raise ValueError("EPC archive manifest is missing its checksum")

    if dataset not in EPC_DATASET_IDS:
        raise ValueError(f"Unsupported EPC dataset: {dataset}")
    formats = EPC_FORMATS if dataset == "domestic" else EPC_NON_DOMESTIC_FORMATS
    dataset_id = EPC_DATASET_IDS[dataset]
    row_counts = {name: 0 for name in formats}
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
            name: [
                member
                for member in csv_members
                if dataset == "non_domestic"
                or Path(member.filename).name.casefold().startswith(name)
            ]
            for name in formats
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
            name = next(
                (
                    name
                    for name in formats
                    if dataset == "non_domestic"
                    or Path(member.filename).name.casefold().startswith(name)
                ),
                None,
            )
            if name is None:
                print(f"Skipping unrecognized EPC CSV member: {member.filename}")
                continue

            period = _publication_period(member.filename)
            source_dataset_id = (
                f"{dataset_id}_{name}" if dataset == "domestic" else dataset_id
            )
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
                            "_source_dataset": source_dataset_id,
                            "_source_file": f"{raw_path}!{member.filename}",
                            "_source_sha256": checksum,
                            "_publication_period": period,
                        },
                    )
                    result = bronze_write(
                        prepared,
                        catalog=RAW_CATALOG,
                        schema=EPC_SCHEMA,
                        table=formats[name],
                        key_cols=["_source_sha256"],
                        batch_id=f"{member_number}-{batch_number}",
                        volume_path=EPC_VOLUME_PATH,
                    )
                    row_counts[name] += result["row_count"]
                    print(
                        f"Wrote EPC batch {batch_number}: "
                        f"{result['row_count']:,} rows "
                        f"({name}: {row_counts[name]:,} total)",
                        flush=True,
                    )

    if not any(row_counts.values()):
        raise ValueError(f"EPC {dataset} archive contained no data rows")
    update_dataset_manifest(
        dataset,
        row_count=sum(row_counts.values()),
        row_counts=row_counts,
    )
    return row_counts


def run(download: bool = True, dataset: str = "domestic") -> dict[str, object]:
    if dataset not in EPC_DATASET_IDS:
        raise ValueError(f"Unsupported EPC dataset: {dataset}")
    dataset_id = EPC_DATASET_IDS[dataset]
    run_id = new_run_id()
    raw_path: Path | None = None
    manifest: dict = {}
    already_retrieved = False
    try:
        if download:
            info = get_full_load_info(dataset)
            already_retrieved = is_download_current(info["lastUpdated"], dataset)
            raw_path = save_downloaded_dataset(info, dataset=dataset)
            manifest = get_dataset_manifest(dataset) or {}
        else:
            raw_path = existing_raw_path(dataset)
            manifest = get_dataset_manifest(dataset) or {}

        if raw_path is None:
            raise RuntimeError("EPC raw archive was not resolved")
        print(f"Ingesting {dataset_id} from {raw_path}")
        row_counts = ingest_archive(raw_path, manifest, dataset)
        row_count = sum(row_counts.values())
        manifest = get_dataset_manifest(dataset) or manifest
        status = "skipped" if already_retrieved else "success"
        for name, dataset_row_count in row_counts.items():
            audit_dataset_id = (
                f"{dataset_id}_{name}" if dataset == "domestic" else dataset_id
            )
            _write_audit_event(
                run_id,
                audit_dataset_id,
                status,
                row_count=dataset_row_count,
                manifest=manifest,
                source=dataset_id,
            )
        append_run_log(
            status,
            dataset_id=dataset_id,
            run_id=run_id,
            last_updated=manifest.get("last_updated"),
            row_count=row_count,
            row_counts=row_counts,
            sha256=manifest.get("sha256"),
        )
        print(f"Completed {dataset_id}: {status}; {row_count:,} rows")
        return {
            "dataset_id": dataset_id,
            "status": status,
            "row_count": row_count,
            "row_counts": row_counts,
        }
    except Exception as error:
        append_run_log(
            "failed",
            dataset_id=dataset_id,
            run_id=run_id,
            error=str(error),
        )
        _write_audit_event(
            run_id,
            dataset_id,
            "failed",
            manifest=manifest,
            error=str(error),
            source=dataset_id,
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
    source: str = EPC_DATASET_ID,
) -> None:
    manifest = manifest or {}
    try:
        write_event(
            run_id,
            dataset_id,
            status,
            source=source,
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
    parser = argparse.ArgumentParser(description="Ingest EPC certificate data")
    parser.add_argument(
        "--dataset",
        choices=tuple(EPC_DATASET_IDS),
        default="domestic",
        help="EPC dataset to ingest",
    )
    parser.add_argument(
        "--no-download",
        action="store_true",
        help="Process the accepted raw EPC archive without checking the API",
    )
    args = parser.parse_args()
    run(download=not args.no_download, dataset=args.dataset)


if __name__ == "__main__":
    main()