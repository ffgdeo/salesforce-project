"""Transformation 2 — Silver layer.

Cleans and standardises a second Salesforce object type
(e.g. Opportunities) for the silver layer.
"""
import dlt
from pyspark.sql import functions as F


@dlt.table(
    name="transformation2_silver",
    comment="Cleaned Salesforce Opportunity records — silver layer",
)
def transformation2_silver():
    df = dlt.read("salesforce_ingestion_bronze")
    return (
        df.filter(F.col("object_type") == "Opportunity")
        .withColumn("close_date", F.to_date("CloseDate", "yyyy-MM-dd"))
        .withColumn("amount", F.col("Amount").cast("double"))
        .select("Id", "Name", "StageName", "close_date", "amount", "AccountId")
    )
