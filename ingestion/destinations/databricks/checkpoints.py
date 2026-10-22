

def has_completed_batch(src_name, file_hash):
    """Check whether a source file has already completed ingestion so reruns do
    not load the same source file twice.

    Args:
        src_name: Source or dataset name.
        file_hash: Content hash identifying the source file.

    Returns:
        A boolean indicating whether the batch is complete.
    """
    pass


def start_batch(src_name, file_hash):
    """Register the start of a Databricks ingestion batch to support recovery,
    monitoring, and distinction between a new file and a retry.

    Args:
        src_name: Source or dataset name.
        file_hash: Content hash identifying the source file.

    Returns:
        The identifier of the started batch.
    """
    pass


def complete_batch(batch_id, row_count):
    """Mark a Databricks ingestion batch as complete. Completion state and row
    counts provide an auditable record of what was written to bronze storage.

    Args:
        batch_id: Identifier of the ingestion batch.
        row_count: Number of rows successfully written.

    Returns:
        None.
    """
    pass


def fail_batch(batch_id, error):
    """Record an error for a failed Databricks ingestion batch so scheduled
    failures are diagnosable and can be safely retried.

    Args:
        batch_id: Identifier of the ingestion batch.
        error: Error details to record.

    Returns:
        None.
    """
    pass