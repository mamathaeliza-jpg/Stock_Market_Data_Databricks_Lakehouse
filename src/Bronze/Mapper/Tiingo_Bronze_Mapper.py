
from pyspark.sql import functions as F

from Bronze.Model import Tiingo_Bronze_Model as M


# ==========================================
# TIINGO BRONZE MAPPER
# ==========================================

def map_to_bronze(raw_df):

    # STEP 1: Explode Tiingo historical prices
    historical_prices = F.explode(
        F.col("payload.historical_prices")
    ).alias("historical_price")

    # STEP 2: Map JSON fields to Bronze columns
    bronze_df = (
        raw_df
        .select(
            F.col("symbol"),
            F.col("source"),
            F.col("batch_id"),
            F.col("ingested_at"),
            historical_prices
        )
        .select(
            F.col("symbol").alias(M.SYMBOL),
            F.to_date(
                F.col("historical_price.date")
            ).alias(M.TRADE_DATE),
            F.col("historical_price.open").alias(M.OPEN),
            F.col("historical_price.high").alias(M.HIGH),
            F.col("historical_price.low").alias(M.LOW),
            F.col("historical_price.close").alias(M.CLOSE),
            F.col("historical_price.volume").alias(M.VOLUME),
            F.col("historical_price.adjOpen").alias(M.ADJ_OPEN),
            F.col("historical_price.adjHigh").alias(M.ADJ_HIGH),
            F.col("historical_price.adjLow").alias(M.ADJ_LOW),
            F.col("historical_price.adjClose").alias(M.ADJ_CLOSE),
            F.col("historical_price.adjVolume").alias(M.ADJ_VOLUME),
            F.col("historical_price.divCash").alias(M.DIV_CASH),
            F.col("historical_price.splitFactor").alias(M.SPLIT_FACTOR),
            F.col("source").alias(M.SOURCE),
            F.col("batch_id").alias(M.BATCH_ID),
            F.col("ingested_at").alias(M.INGESTED_AT)
        )
    )

    # STEP 3: Add Bronze load timestamp
    bronze_df = bronze_df.withColumn(
        M.BRONZE_LOADED_AT,
        F.current_timestamp()
    )

    # STEP 4: Return Bronze columns in model order
    return bronze_df.select(*M.BRONZE_COLUMNS)
