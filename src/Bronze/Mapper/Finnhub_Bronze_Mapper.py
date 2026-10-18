from pyspark.sql import functions as F
from Bronze.Model import Finnhub_Bronze_Model as M


def map_to_bronze(raw_df):
    """
    Convert the Finnhub Microsoft stock quote
    into one Bronze row per API response.
    """

    return (
        raw_df
        .select(
            F.col("symbol"),
            F.col("source"),
            F.col("batch_id"),
            F.col("ingested_at"),
            F.col("payload"),
        )
        .select(
            F.col("symbol").alias(M.SYMBOL),

            F.from_unixtime(
                F.col("payload.t").cast("long")
            ).cast("timestamp").alias(M.QUOTE_TIMESTAMP),

            F.col("payload.c").alias(M.CURRENT_PRICE),

            F.col("payload.o").alias(M.OPEN_PRICE),

            F.col("payload.h").alias(M.HIGH_PRICE),

            F.col("payload.l").alias(M.LOW_PRICE),

            F.col("payload.pc").alias(M.PREVIOUS_CLOSE),

            F.col("payload.d").alias(M.PRICE_CHANGE),

            F.col("payload.dp").alias(M.PERCENT_CHANGE),

            F.col("source").alias(M.SOURCE),

            F.col("batch_id").alias(M.BATCH_ID),

            F.col("ingested_at").alias(M.INGESTED_AT),
        )
        .withColumn(
            M.BRONZE_LOADED_AT,
            F.current_timestamp()
        )
    )
