
from datetime import datetime, timezone


def add_ingestion_metadata(df, metadata: dict):
    result = df.copy()
    result["_ingested_at"] = datetime.now(timezone.utc)
    for key, value in metadata.items():
        result[key] = value
    return result