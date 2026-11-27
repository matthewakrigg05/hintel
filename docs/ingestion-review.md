# Ingestion Review Notes

## Scope

This review covers the Databricks ingestion destination, the pipeline callers,
and audit logging. It focuses on correctness, idempotency, failure handling,
data preservation, and operational behavior.

## Good Patterns

- Identifier and escaped column-name validation help prevent SQL injection and
  malformed Databricks identifiers.
- Managed Volume uploads use checksum-derived filenames and batch identifiers,
  allowing `COPY INTO` history to reduce duplicate loading.
- The source checksum is a consistent part of bronze metadata, making uploads
  traceable back to accepted source files.
- Large EPC and Price Paid datasets are chunked before upload, keeping individual
  CSV sizes controlled.
- Successful and failed pipeline outcomes have both local JSONL logs and
  Databricks audit events.
- Scottish EPC archive extraction validates member paths before extraction.
- Raw Delta tables preserve source columns as strings, leaving type conversion
  to dbt and the Silver layer.
- The existing dbt project already separates Bronze sources, Silver staging,
  and Gold marts.

Relevant implementation:

- [Databricks destination](../ingestion/destinations/databricks/databricks.py)
- [Databricks audit logging](../ingestion/destinations/databricks/audit.py)
- [Price Paid pipeline](../ingestion/land_registry/price_paid/pipeline.py)
- [HPI pipeline](../ingestion/land_registry/hpi/pipeline.py)
- [England and Wales EPC pipeline](../ingestion/epc/england_wales/pipeline.py)
- [Scottish EPC pipeline](../ingestion/epc/scotland/pipeline.py)

## Critical Action

### Reset mode can delete already loaded batches

`DATABRICKS_RESET_RAW_TABLES=true` executes `DROP TABLE IF EXISTS` inside
`bronze_write()`. Pipelines call this function repeatedly for HPI datasets
and in batches for Price Paid and EPC data.

If reset mode is enabled while multiple batches are written, a later batch can
drop the table created or populated by an earlier batch.

**Action:** perform schema reset once before a pipeline run, or replace reset
mode with an explicit schema rebuild command. Do not leave batch-level reset
behavior enabled for normal ingestion.

## High-Priority Actions

### Configure CSV loading options explicitly

The `COPY INTO` statement currently specifies only `header=true`. Add explicit
CSV options for multiline fields, quote characters, escapes, nulls, and malformed
records where source behavior requires them.

This is especially important for addresses and free-text EPC fields that can
contain embedded quotes or line breaks.

### Retry HTTP 429 and 5xx upload responses

Volume upload retries currently cover `requests.ConnectionError` and
`requests.Timeout`. HTTP 429 and 5xx responses raise immediately through
`response.raise_for_status()` rather than entering the retry loop.

**Action:** classify transient HTTP status codes and retry them with backoff.
Retain final status and response text in the raised error.

### Add safe schema evolution

Creating a table with `CREATE TABLE IF NOT EXISTS` does not reconcile source
columns when an upstream schema changes. Databricks may then:

- leave the table with an outdated column set
- reject a `COPY INTO` operation
- silently preserve an unintended schema depending on load options

**Action:** compare incoming columns with the target schema before loading.
Require an explicit schema policy for added, removed, or renamed columns.

### Broaden audit fallback handling

Some pipeline audit helpers catch only a narrow set of exceptions. Databricks
SQL errors outside that set could stop an otherwise successful ingestion run.

**Action:** centralize audit handling and keep the local JSONL outcome as the
durable fallback for all audit-layer failures.

## Medium-Priority Actions

- Validate upload and copy operation state with explicit stage names in logs.
- Preserve final HTTP status and Databricks SQL error details in pipeline
  failures.
- Centralize Volume and audit connection configuration instead of reading
  environment variables across modules.
- Avoid repeated audit-table creation work for every event in a pipeline run.
- Improve the audit table-name validator and quote all identifier components.
- Add an earlier preflight for required credentials, catalog permission,
  schema permission, and Volume existence.
- Add a retention or cleanup policy for uploaded Volume CSV files.

## Future Enhancements

- Introduce explicit Bronze schema evolution policies.
- Add publication lineage fields linking Bronze source files to dbt models and
  database copies.
- Add operational stages for download, Bronze upload, COPY INTO, dbt build,
  and PostgreSQL publication.
- Publish aggregate and transaction models from Databricks Gold rather than
  loading raw source fields directly into the website database.
- Consider staged-file lifecycle management as ingestion volume grows.

## Priority Summary

| Priority | Action |
| --- | --- |
| Critical | Move reset behavior outside the per-batch write operation. |
| High | Define CSV options explicitly. |
| High | Retry transient HTTP 429 and 5xx responses. |
| High | Detect and reconcile schema changes safely. |
| High | Make audit fallback consistent across pipelines. |
| Medium | Improve preflight checks and operational diagnostics. |
| Future | Establish automated Volume retention and publication lineage. |
