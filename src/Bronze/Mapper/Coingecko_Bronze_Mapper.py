
from pyspark.sql import functions as F

from Bronze.Model import CoinGecko_Bronze_Model as M


# ==========================================
# COINGECKO BRONZE MAPPER
# ==========================================

def map_to_bronze(raw_df):

    # STEP 1: Explode CoinGecko markets
    markets = F.explode(
        F.col("payload.markets")
    ).alias("market")

    # STEP 2: Map JSON fields to Bronze columns
    bronze_df = (
        raw_df
        .select(
            F.col("currency"),
            F.col("source"),
            F.col("batch_id"),
            F.col("ingested_at"),
            markets
        )
        .select(
            F.col("market.id").alias(M.COIN_ID),
            F.col("market.symbol").alias(M.SYMBOL),
            F.col("market.name").alias(M.NAME),
            F.col("currency").alias(M.CURRENCY),
            F.col("market.current_price").alias(M.CURRENT_PRICE),
            F.col("market.market_cap").alias(M.MARKET_CAP),
            F.col("market.total_volume").alias(M.TOTAL_VOLUME),
            F.col("market.price_change_24h").alias(M.PRICE_CHANGE_24H),
            F.col("market.price_change_percentage_24h").alias(
                M.PRICE_CHANGE_PERCENTAGE_24H
            ),
            F.col("market.last_updated").alias(M.LAST_UPDATED),
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
