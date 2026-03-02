"""Transformation 2 — Gold layer.

Aggregates Opportunity data into pipeline and revenue summaries.
"""
import dlt
from pyspark.sql import functions as F


@dlt.table(
    name="transformation2_gold",
    comment="Opportunity pipeline summary by account and stage — gold layer",
)
def transformation2_gold():
    df = dlt.read("transformation2_silver")
    return (
        df.groupBy("AccountId", "StageName")
        .agg(
            F.count("Id").alias("opportunity_count"),
            F.sum("amount").alias("total_amount"),
            F.max("close_date").alias("latest_close_date"),
        )
        .orderBy("AccountId", "StageName")
    )
