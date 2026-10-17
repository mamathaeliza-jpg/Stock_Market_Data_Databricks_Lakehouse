
from pyspark.sql import functions as F

from Bronze.Model import Alpha_Model as M


def map_to_bronze(raw_df):
    """
    Convert the Alpha Vantage daily time-series map
    into one row per trading date.
    """

    daily_prices = F.explode(
        F.map_entries(
            F.col("payload").getField("Time Series (Daily)")
        )
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
            F.col("daily_price.key").alias(M.TRADE_DATE),
            F.col("daily_price.value").getItem("1. open").alias(M.OPEN),
            F.col("daily_price.value").getItem("2. high").alias(M.HIGH),
            F.col("daily_price.value").getItem("3. low").alias(M.LOW),
            F.col("daily_price.value").getItem("4. close").alias(M.CLOSE),
            F.col("daily_price.value").getItem("5. volume").alias(M.VOLUME),
            F.col("source").alias(M.SOURCE),
            F.col("batch_id").alias(M.BATCH_ID),
            F.col("ingested_at").alias(M.INGESTED_AT),
        )
        .withColumn(
            M.BRONZE_LOADED_AT,
            F.current_timestamp()
        )
    )
