
from pyspark.sql import functions as F

from Bronze.Model import Nvda_Bronze_Model as M


def map_to_bronze(raw_df):
    """
    Convert the Twelve Data NVIDIA (NVDA)
    daily time-series array into one row
    per trading date.
    """

    daily_prices = F.explode(
        F.col("payload.values")
    ).alias("daily_price")

    return (
        raw_df
        .select(
            F.col("symbol"),
            F.col("source"),
            F.col("batch_id"),
            F.col("ingested_at"),
            daily_prices,
        )
        .select(
            F.col("symbol").alias(M.SYMBOL),

            F.col("daily_price.datetime").alias(M.TRADE_DATE),

            F.col("daily_price.open").alias(M.OPEN),

            F.col("daily_price.high").alias(M.HIGH),

            F.col("daily_price.low").alias(M.LOW),

            F.col("daily_price.close").alias(M.CLOSE),

            F.col("daily_price.volume").alias(M.VOLUME),

            F.col("source").alias(M.SOURCE),

            F.col("batch_id").alias(M.BATCH_ID),

            F.col("ingested_at").alias(M.INGESTED_AT),
        )
        .withColumn(
            M.BRONZE_LOADED_AT,
            F.current_timestamp()
        )
    )
