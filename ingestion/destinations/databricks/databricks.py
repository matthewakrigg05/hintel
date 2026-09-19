

def bronze_write(df, catalog, schema, table, key_cols):
    """Write a dataframe to a Databricks bronze table. A destination adapter
    keeps Databricks-specific write and merge behavior out of source extractors
    and shared ingestion utilities.

    Args:
        df: Dataframe to write.
        catalog: Databricks catalog containing the target table.
        schema: Databricks schema containing the target table.
        table: Target bronze table name.
        key_cols: Columns used to identify records during the write.

    Returns:
        The result of the Databricks write operation.
    """
    pass