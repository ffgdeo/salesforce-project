"""Transformation 1 — Silver layer.

Cleans and standardises raw Salesforce data ingested to bronze,
producing a curated silver table.
"""
import sys
sys.path.append("../shared")

import dlt
from pyspark.sql import functions as F
from utils import get_bronze_table

catalog = spark.conf.get("catalog")
bronze_schema = spark.conf.get("bronze_schema")
silver_schema = spark.conf.get("silver_schema")

@dlt.table(
    name=f"{catalog}.{silver_schema}.transformation1_silver",
    comment="Cleaned and standardised Salesforce records — silver layer",
)
def transformation1_silver():
    df = spark.readStream.table(f"{catalog}.{bronze_schema}.account")
    return (
        df.withColumn("ingested_at", F.current_timestamp())
    )
