"""Databricks audit logging for ingestion runs."""

import os
import uuid
from datetime import datetime, timezone


AUDIT_TABLE = os.getenv("DATABRICKS_INGESTION_AUDIT_TABLE", "bronze.ingestion_runs")


def new_run_id() -> str:
    """Return a unique identifier shared by one pipeline execution."""
    return str(uuid.uuid4())


def _connection():
    try:
        from databricks import sql
    except ImportError as error:
        raise RuntimeError("Install databricks-sql-connector to enable Databricks audit logging") from error

    required = {
        "server_hostname": os.getenv("DATABRICKS_SERVER_HOSTNAME"),
        "http_path": os.getenv("DATABRICKS_HTTP_PATH"),
        "access_token": os.getenv("DATABRICKS_TOKEN"),
    }
    missing = [name for name, value in required.items() if not value]
    if missing:
        raise RuntimeError(f"Missing Databricks audit configuration: {', '.join(missing)}")
    return sql.connect(**required)


def _table_name() -> str:
    if not AUDIT_TABLE.replace("_", "").replace(".", "").isalnum():
        raise ValueError("DATABRICKS_INGESTION_AUDIT_TABLE contains invalid characters")
    return AUDIT_TABLE


def ensure_audit_table() -> None:
    """Create the Delta audit table when it does not already exist."""
    statement = f"""
        create table if not exists {_table_name()} (
            run_id string,
            dataset_id string,
            source string,
            source_url string,
            publication_period string,
            status string,
            started_at timestamp,
            completed_at timestamp,
            row_count bigint,
            file_size_bytes bigint,
            sha256 string,
            error_message string
        ) using delta
    """
    connection = _connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(statement)
    finally:
        connection.close()


def write_event(
    run_id: str,
    dataset_id: str,
    status: str,
    *,
    source_url: str | None = None,
    publication_period: str | None = None,
    started_at: datetime | None = None,
    completed_at: datetime | None = None,
    row_count: int | None = None,
    file_size_bytes: int | None = None,
    sha256: str | None = None,
    error_message: str | None = None,
) -> None:
    """Write one ingestion event to the Databricks Delta audit table."""
    ensure_audit_table()
    statement = f"""
        insert into {_table_name()} values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    values = (
        run_id,
        dataset_id,
        "land_registry_hpi",
        source_url,
        publication_period,
        status,
        started_at or datetime.now(timezone.utc),
        completed_at,
        row_count,
        file_size_bytes,
        sha256,
        error_message,
    )
    connection = _connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(statement, values)
    finally:
        connection.close()