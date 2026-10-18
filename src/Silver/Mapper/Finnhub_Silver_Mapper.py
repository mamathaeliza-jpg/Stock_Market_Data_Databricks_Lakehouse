
from pyspark.sql import functions as F

from Silver.Model import Finnhub_Silver_Model as M


def map_to_silver(bronze_df):
    """
    Clean and transform Finnhub Microsoft (MSFT)
    Bronze stock quotes into Silver records.
    """

    return (
        bronze_df
        .select(
            F.col("symbol").alias(M.SYMBOL),

            F.to_date(
                F.col("quote_timestamp")
            ).alias(M.TRADE_DATE),

            F.col("quote_timestamp")
            .cast("timestamp")
            .alias(M.QUOTE_TIMESTAMP),

            F.col("current_price")
            .cast("double")
            .alias(M.CURRENT_PRICE),

            F.col("open_price")
            .cast("double")
            .alias(M.OPEN_PRICE),

            F.col("high_price")
            .cast("double")
            .alias(M.HIGH_PRICE),

            F.col("low_price")
            .cast("double")
            .alias(M.LOW_PRICE),

            F.col("previous_close")
            .cast("double")
            .alias(M.PREVIOUS_CLOSE),

            F.col("price_change")
            .cast("double")
            .alias(M.PRICE_CHANGE),

            F.col("percent_change")
            .cast("double")
            .alias(M.PERCENT_CHANGE),

            F.col("source").alias(M.SOURCE),

            F.col("batch_id").alias(M.BATCH_ID),

            F.col("ingested_at")
            .cast("timestamp")
            .alias(M.INGESTED_AT),

            F.col("bronze_loaded_at")
            .cast("timestamp")
            .alias(M.BRONZE_LOADED_AT),
        )
        .filter(
            (F.col(M.SYMBOL) == "MSFT")
            & F.col(M.QUOTE_TIMESTAMP).isNotNull()
            & (F.col(M.CURRENT_PRICE) > 0)
        )
        .dropDuplicates(
            [
                M.SYMBOL,
                M.SOURCE,
                M.QUOTE_TIMESTAMP,
            ]
        )
        .withColumn(
            M.SILVER_LOADED_AT,
            F.current_timestamp()
        )
        .select(*M.SILVER_COLUMNS)
    )
