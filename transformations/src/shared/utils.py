"""Shared utility functions for Salesforce transformations."""


def get_bronze_table(spark, table_name: str, catalog: str = "main", schema: str = "bronze"):
    """Read a bronze table from Unity Catalog."""
    return spark.table(f"{catalog}.{schema}.{table_name}")


def write_silver(df, table_name: str, catalog: str = "main", schema: str = "silver"):
    """Write a DataFrame to a silver table using Delta merge-on-read."""
    df.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.{table_name}")


def write_gold(df, table_name: str, catalog: str = "main", schema: str = "gold"):
    """Write a DataFrame to a gold table."""
    df.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.{table_name}")
