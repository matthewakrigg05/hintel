import argparse
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath, PureWindowsPath

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

import pandas as pd
import py7zr

from ingestion.common.metadata import add_ingestion_metadata
from ingestion.destinations.databricks.audit import new_run_id, write_event
from ingestion.destinations.databricks.databricks import RAW_CATALOG, bronze_write

if __package__ in {None, ""}:
    from ingestion.epc.scotland.config import EPC_BATCH_ROWS, EPC_SCHEMA, SCOTLAND_DATASETS
    from ingestion.epc.scotland.downloads import (
        append_run_log,
        existing_raw_path,
        get_dataset_manifest,
        save_downloaded_dataset,
        update_dataset_manifest,
    )
else:
    from .config import EPC_BATCH_ROWS, EPC_SCHEMA, SCOTLAND_DATASETS
    from .downloads import (
        append_run_log,
        existing_raw_path,
        get_dataset_manifest,
        save_downloaded_dataset,
        update_dataset_manifest,
    )


EPC_VOLUME_PATH = os.getenv(
    "DATABRICKS_EPC_RAW_VOLUME_PATH",
    f"/Volumes/{RAW_CATALOG}/{EPC_SCHEMA}/files",
)


def _csv_members(raw_path: Path, destination: Path) -> list[Path]:
    with py7zr.SevenZipFile(raw_path, mode="r") as archive:
        members = [
            name for name in archive.getnames() if name.lower().endswith(".csv")
        ]
        for member in members:
            normalized = PurePosixPath(member.replace("\\", "/"))
            windows_path = PureWindowsPath(member)
            if (
                normalized.is_absolute()
                or windows_path.is_absolute()
                or windows_path.drive
                or ".." in normalized.parts
            ):
                raise ValueError(f"Unsafe path in Scottish EPC archive: {member}")
        if not members:
            raise ValueError("Scottish EPC archive contains no CSV files")
        archive.extract(path=destination, targets=members)

    paths = [
        destination.joinpath(*PurePosixPath(member.replace("\\", "/")).parts)
        for member in members
    ]
    missing = [path for path in paths if not path.is_file()]
    if missing:
        raise ValueError(
            "Scottish EPC archive did not extract expected CSV files: "
            + ", ".join(str(path) for path in missing)
        )
    return paths


def ingest_archive(raw_path: Path, manifest: dict, dataset: str) -> int:
    definition = SCOTLAND_DATASETS.get(dataset)
    if definition is None:
        raise ValueError(f"Unsupported Scottish EPC dataset: {dataset}")
    checksum = manifest.get("sha256")
    if not checksum:
        raise ValueError("Scottish EPC archive manifest is missing its checksum")

    row_count = 0
    with tempfile.TemporaryDirectory(prefix="epc-scotland-") as temporary_dir:
        members = _csv_members(raw_path, Path(temporary_dir))
        for member_number, member_path in enumerate(members, start=1):
            print(
                f"Processing Scottish EPC archive member "
                f"{member_number}/{len(members)}: {member_path.name}"
            )
            chunks = pd.read_csv(
                member_path,
                dtype=str,
                encoding="utf-8-sig",
                chunksize=EPC_BATCH_ROWS,
                low_memory=False,
            )
            for batch_number, frame in enumerate(chunks, start=1):
                prepared = add_ingestion_metadata(
                    frame,
                    {
                        "_source_dataset": f"epc_scotland_{dataset}",
                        "_source_file": f"{raw_path}!{member_path.name}",
                        "_source_sha256": checksum,
                        "_publication_period": manifest.get("last_updated"),
                    },
                )
                result = bronze_write(
                    prepared,
                    catalog=RAW_CATALOG,
                    schema=EPC_SCHEMA,
                    table=definition["table"],
                    key_cols=["_source_sha256"],
                    batch_id=f"{member_number}-{batch_number}",
                    volume_path=EPC_VOLUME_PATH,
                )
                row_count += result["row_count"]
                print(
                    f"Wrote Scottish EPC batch {member_number}-{batch_number}: "
                    f"{result['row_count']:,} rows "
                    f"({row_count:,} total)",
                    flush=True,
                )
    if not row_count:
        raise ValueError(f"Scottish EPC {dataset} archive contained no data rows")

    update_dataset_manifest(dataset, row_count=row_count)
    return row_count


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
            source="epc_scotland",
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


def run(
    download: bool = True,
    dataset: str = "all",
    refresh: bool = False,
) -> dict[str, int]:
    if dataset != "all" and dataset not in SCOTLAND_DATASETS:
        raise ValueError(f"Unsupported Scottish EPC dataset: {dataset}")
    if refresh and not download:
        raise ValueError("--refresh cannot be used with --no-download")
    datasets = tuple(SCOTLAND_DATASETS) if dataset == "all" else (dataset,)
    run_id = new_run_id()
    row_counts = {}

    for name in datasets:
        dataset_id = f"epc_scotland_{name}"
        manifest: dict = {}
        try:
            if download:
                raw_path = save_downloaded_dataset(name, refresh=refresh)
            else:
                raw_path = existing_raw_path(name)
            manifest = get_dataset_manifest(name) or {}
            print(f"Ingesting {dataset_id} from {raw_path}")
            count = ingest_archive(raw_path, manifest, name)
            row_counts[name] = count
            status = "success"
            _write_audit_event(
                run_id,
                dataset_id,
                status,
                row_count=count,
                manifest=manifest,
            )
            append_run_log(
                status,
                dataset_id=dataset_id,
                run_id=run_id,
                row_count=count,
                sha256=manifest.get("sha256"),
            )
            print(f"Completed {dataset_id}: {status}; {count:,} rows")
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
            )
            raise
    return row_counts


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest Scottish EPC datasets")
    parser.add_argument(
        "--dataset",
        choices=("all", *SCOTLAND_DATASETS),
        default="all",
        help="Scottish EPC resource to ingest",
    )
    parser.add_argument(
        "--no-download",
        action="store_true",
        help="Process accepted archives without checking the download URLs",
    )
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Download resources again even when an accepted archive exists",
    )
    args = parser.parse_args()
    run(
        download=not args.no_download,
        dataset=args.dataset,
        refresh=args.refresh,
    )


if __name__ == "__main__":
    main()
