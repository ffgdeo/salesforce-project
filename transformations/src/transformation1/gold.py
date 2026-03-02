"""Transformation 1 — Gold layer.

Aggregates silver data into business-ready gold tables.
"""
import dlt
from pyspark.sql import functions as F

catalog = spark.conf.get("catalog")
silver_schema = spark.conf.get("silver_schema")
gold_schema = spark.conf.get("gold_schema")


@dlt.table(
    name=f"{catalog}.{gold_schema}.transformation1_gold",
    comment="Business-level aggregation of transformation1 silver data",
)
def transformation1_gold():
    df = spark.read.table(f"{catalog}.{silver_schema}.transformation1_silver")
    return (
        df.groupBy("Industry")
        .agg(
            F.count("*").alias("record_count"),
            F.max("ingested_at").alias("last_updated"),
        )
    )