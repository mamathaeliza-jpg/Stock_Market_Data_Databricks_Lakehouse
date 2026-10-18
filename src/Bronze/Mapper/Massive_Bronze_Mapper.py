
from pyspark.sql import functions as F

from Bronze.Model import Massive_Bronze_Model as M


# ==========================================
# MASSIVE BRONZE MAPPER
# ==========================================

def map_to_bronze(raw_df):

    # STEP 1: Explode Massive daily prices
    daily_prices = F.explode(
        F.col("payload.daily_prices.results")
    ).alias("daily_price")

    # STEP 2: Map JSON fields to Bronze columns
    bronze_df = (
        raw_df
        .select(
            F.col("symbol"),
            F.col("source"),
            F.col("batch_id"),
            F.col("ingested_at"),
            daily_prices
        )
        .select(
            F.col("symbol").alias(M.SYMBOL),
            F.to_date(
                F.from_unixtime(
                    (F.col("daily_price.t") / 1000).cast("long"),
                    "yyyy-MM-dd"
                )
            ).alias(M.TRADE_DATE),
            F.col("daily_price.o").cast("double").alias(M.OPEN),
            F.col("daily_price.h").cast("double").alias(M.HIGH),
            F.col("daily_price.l").cast("double").alias(M.LOW),
            F.col("daily_price.c").cast("double").alias(M.CLOSE),
            F.col("daily_price.v").cast("long").alias(M.VOLUME),
            F.col("daily_price.vw").cast("double").alias(M.VWAP),
            F.col("daily_price.n").cast("long").alias(M.TRANSACTIONS),
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
