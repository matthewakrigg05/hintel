import argparse
import os
import re
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from ingestion.common.metadata import add_ingestion_metadata
from ingestion.destinations.databricks.audit import new_run_id, write_event
from ingestion.destinations.databricks.databricks import RAW_CATALOG, bronze_write

from .config import EPC_BATCH_ROWS, EPC_DATASET_ID, EPC_SCHEMA, EPC_TABLE
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


def _publication_period(member: str) -> str | None:
    match = re.search(r"(?:19|20)\d{2}", Path(member).stem)
    return match.group(0) if match else None


def ingest_archive(raw_path: Path, manifest: dict, run_id: str) -> int:
    checksum = manifest.get("sha256")
    if not checksum:
        raise ValueError("EPC archive manifest is missing its checksum")

    total_rows = 0
    batch_number = 0
    with zipfile.ZipFile(raw_path) as archive:
        members = [
            member
            for member in archive.infolist()
            if not member.is_dir() and member.filename.lower().endswith(".csv")
        ]
        if not members:
            raise ValueError("EPC archive contains no CSV files")

        for member_number, member in enumerate(members, start=1):
            period = _publication_period(member.filename)
            print(f"Processing EPC archive member {member_number}/{len(members)}: {member.filename}")
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
                            "_source_dataset": EPC_DATASET_ID,
                            "_source_file": f"{raw_path}!{member.filename}",
                            "_source_sha256": checksum,
                            "_publication_period": period,
                        },
                    )
                    result = bronze_write(
                        prepared,
                        catalog=RAW_CATALOG,
                        schema=EPC_SCHEMA,
                        table=EPC_TABLE,
                        key_cols=["_source_sha256"],
                        batch_id=f"{member_number}-{batch_number}",
                        volume_path=EPC_VOLUME_PATH,
                    )
                    total_rows += result["row_count"]
                    print(
                        f"Wrote EPC batch {batch_number}: "
                        f"{result['row_count']:,} rows (total {total_rows:,})",
                        flush=True,
                    )

    if not total_rows:
        raise ValueError("EPC archive contained no certificate rows")
    update_dataset_manifest(row_count=total_rows)
    return total_rows


def run(download: bool = True) -> dict[str, object]:
    run_id = new_run_id()
    raw_path: Path | None = None
    manifest: dict = {}
    try:
        if download:
            info = get_full_load_info()
            already_retrieved = is_download_current(info["lastUpdated"])
            raw_path = save_downloaded_dataset(info)
            manifest = get_dataset_manifest() or {}
            if already_retrieved:
                status = "skipped"
                row_count = manifest.get("row_count", 0)
                _write_audit_event(run_id, status, row_count=row_count, manifest=manifest)
                append_run_log(
                    status,
                    run_id=run_id,
                    last_updated=info["lastUpdated"],
                    row_count=row_count,
                )
                print(f"Completed {EPC_DATASET_ID}: {status}; source file is unchanged")
                return {"dataset_id": EPC_DATASET_ID, "status": status, "row_count": row_count}
        else:
            raw_path = existing_raw_path()
            manifest = get_dataset_manifest() or {}

        if raw_path is None:
            raise RuntimeError("EPC raw archive was not resolved")
        print(f"Ingesting {EPC_DATASET_ID} from {raw_path}")
        row_count = ingest_archive(raw_path, manifest, run_id)
        manifest = get_dataset_manifest() or manifest
        _write_audit_event(run_id, "success", row_count=row_count, manifest=manifest)
        append_run_log(
            "success",
            run_id=run_id,
            last_updated=manifest.get("last_updated"),
            row_count=row_count,
            sha256=manifest.get("sha256"),
        )
        print(f"Completed {EPC_DATASET_ID}: {row_count:,} rows")
        return {"dataset_id": EPC_DATASET_ID, "status": "success", "row_count": row_count}
    except Exception as error:
        _write_audit_event(run_id, "failed", manifest=manifest, error=str(error))
        append_run_log("failed", run_id=run_id, error=str(error))
        raise


def _write_audit_event(
    run_id: str,
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
            EPC_DATASET_ID,
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