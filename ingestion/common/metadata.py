
from datetime import datetime, timezone


def add_ingestion_metadata(df, metadata: dict):
    """
    Return a dataframe with ingestion timestamp and source metadata, making it
    possible to identify when and from which source file each record was produced.

    Args:
        df: Dataframe to copy and annotate.
        metadata: Column names and values to add to the dataframe.

    Returns:
        A copy of the dataframe containing the added metadata columns.
    """
    result = df.copy()
    result["_ingested_at"] = datetime.now(timezone.utc)
    for key, value in metadata.items():
        result[key] = value
    return result