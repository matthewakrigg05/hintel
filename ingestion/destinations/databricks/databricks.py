"""Bulk Databricks Volume uploads and COPY INTO ingestion."""

import os
import re
import tempfile
import time
from pathlib import Path
from urllib.parse import quote

import pandas as pd
import requests
from dotenv import load_dotenv

from .audit import _connection


load_dotenv()

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
RAW_CATALOG = os.getenv("DATABRICKS_RAW_CATALOG", "bronze")
RAW_SCHEMA = os.getenv("DATABRICKS_RAW_SCHEMA", "land_registry")
RAW_VOLUME_PATH = os.getenv(
    "DATABRICKS_RAW_VOLUME_PATH",
    f"/Volumes/{RAW_CATALOG}/{RAW_SCHEMA}/files",
)
RESET_RAW_TABLES = os.getenv("DATABRICKS_RESET_RAW_TABLES", "false").lower() in {
    "1",
    "true",
    "yes",
}
UPLOAD_TIMEOUT_SECONDS = int(os.getenv("DATABRICKS_UPLOAD_TIMEOUT_SECONDS", "1800"))


def _identifier(value: str, label: str) -> str:
    if not isinstance(value, str) or not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"Invalid Databricks {label}: {value!r}")
    return value


def _column_identifier(value: object) -> str:
    column = str(value).strip()
    if not column:
        raise ValueError("Dataframe contains an empty column name")
    return f"`{column.replace('`', '``')}`"


def _volume_path(dataset: str, checksum: str, batch_id: str | None = None) -> str:
    volume_path = RAW_VOLUME_PATH.rstrip("/")
    suffix = f"-{batch_id}" if batch_id else ""
    return f"{volume_path}/{dataset}-{checksum}{suffix}.csv"


def _upload_to_volume(local_path: Path, volume_path: str) -> None:
    hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
    token = os.getenv("DATABRICKS_TOKEN")
    if not hostname or not token:
        raise RuntimeError(
            "DATABRICKS_SERVER_HOSTNAME and DATABRICKS_TOKEN are required "
            "for Volume uploads"
        )

    if not hostname.startswith("http"):
        hostname = f"https://{hostname}"
    encoded_path = quote(volume_path.lstrip("/"), safe="/")
    endpoint = f"{hostname.rstrip('/')}/api/2.0/fs/files/{encoded_path}?overwrite=true"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/octet-stream",
        "Content-Length": str(local_path.stat().st_size),
    }
    last_error = None
    for attempt in range(1, 4):
        try:
            print(
                f"[bronze] uploading {local_path.stat().st_size:,} bytes "
                f"(attempt {attempt}/3)",
                flush=True,
            )
            with local_path.open("rb") as payload:
                response = requests.put(
                    endpoint,
                    headers=headers,
                    data=payload,
                    timeout=(30, UPLOAD_TIMEOUT_SECONDS),
                )
            response.raise_for_status()
            return
        except (requests.ConnectionError, requests.Timeout) as error:
            last_error = error
            if attempt == 3:
                break
            wait_seconds = 2 ** (attempt - 1)
            print(
                f"[bronze] Volume upload reset; retrying in {wait_seconds}s "
                f"(attempt {attempt}/3)",
                flush=True,
            )
            time.sleep(wait_seconds)
    raise RuntimeError(f"Databricks Volume upload failed after 3 attempts: {last_error}") from last_error


def bronze_write(df, catalog, schema, table, key_cols, batch_id: str | None = None):
    """Upload one dataframe to a managed Volume and bulk-load it with COPY INTO.

    ``key_cols`` is retained for the destination interface. Databricks tracks
    the uploaded checksum-named file for COPY INTO idempotency, so no row-level
    delete or insert statements are needed.
    """
    if df.empty:
        print(f"[bronze] {table}: no rows to write", flush=True)
        return {"row_count": 0, "table": f"{catalog}.{schema}.{table}"}

    catalog = _identifier(catalog, "catalog")
    schema = _identifier(schema, "schema")
    table = _identifier(table, "table")
    columns = [str(column) for column in df.columns]
    if len(columns) != len(set(columns)):
        raise ValueError("Dataframe columns must be unique")

    checksum_values = df["_source_sha256"].dropna().astype(str).unique()
    if len(checksum_values) != 1:
        raise ValueError("Each bronze write must contain exactly one source checksum")
    checksum = checksum_values[0]
    if not re.fullmatch(r"[A-Za-z0-9]+", checksum):
        raise ValueError("Source checksum contains invalid filename characters")

    qualified_table = f"`{catalog}`.`{schema}`.`{table}`"
    definitions = ", ".join(f"{_column_identifier(column)} string" for column in columns)
    if batch_id is not None and not re.fullmatch(r"[A-Za-z0-9_-]+", batch_id):
        raise ValueError("Batch identifier contains invalid filename characters")
    target_volume_path = _volume_path(table, checksum, batch_id=batch_id)
    started_at = time.perf_counter()
    print(
        f"[bronze] {table}: preparing {len(df):,} rows for managed Volume upload",
        flush=True,
    )

    connection = _connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"create catalog if not exists `{catalog}`")
            cursor.execute(f"create schema if not exists `{catalog}`.`{schema}`")
            cursor.execute(f"create volume if not exists `{catalog}`.`{schema}`.`files`")
            if RESET_RAW_TABLES:
                print(f"[bronze] {table}: resetting existing table schema", flush=True)
                cursor.execute(f"drop table if exists {qualified_table}")
            cursor.execute(f"create table if not exists {qualified_table} ({definitions}) using delta")
            print(f"[bronze] {table}: Databricks Volume ready", flush=True)

            with tempfile.TemporaryDirectory(prefix="hintel-bronze-") as temporary_dir:
                batch_suffix = f"-{batch_id}" if batch_id else ""
                local_path = Path(temporary_dir) / f"{table}-{checksum}{batch_suffix}.csv"
                df.to_csv(local_path, index=False)
                print(
                    f"[bronze] {table}: uploading {local_path.stat().st_size:,} bytes",
                    flush=True,
                )
                _upload_to_volume(local_path, target_volume_path)

            print(f"[bronze] {table}: issuing COPY INTO", flush=True)
            escaped_path = target_volume_path.replace("'", "''")
            cursor.execute(
                f"""
                copy into {qualified_table}
                from '{escaped_path}'
                fileformat = csv
                format_options ('header' = 'true')
                """
            )
    finally:
        connection.close()

    elapsed = time.perf_counter() - started_at
    print(f"[bronze] {table}: complete in {elapsed:.1f}s", flush=True)
    return {"row_count": len(df), "table": f"{catalog}.{schema}.{table}"}
