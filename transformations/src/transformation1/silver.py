"""Transformation 1 — Silver layer.

Cleans and standardises raw Salesforce data ingested to bronze,
producing a curated silver table.
"""
import sys
sys.path.append("../shared")

import dlt
from pyspark.sql import functions as F
from utils import get_bronze_table


@dlt.table(
    name="transformation1_silver",
    comment="Cleaned and standardised Salesforce records — silver layer",
)
def transformation1_silver():
    df = dlt.read("salesforce_ingestion_bronze")
    return (
        df.filter(F.col("_deleted").isNull() | (F.col("_deleted") == False))
        .withColumn("ingested_at", F.col("_ingested_at").cast("timestamp"))
        .drop("_deleted", "_ingested_at")
    )
