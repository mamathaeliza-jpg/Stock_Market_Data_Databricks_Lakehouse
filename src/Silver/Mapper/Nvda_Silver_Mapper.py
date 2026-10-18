
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from Silver.Model import Nvda_Silver_Model as M


def map_to_silver(bronze_df):
    """
    Clean Twelve Data NVIDIA (NVDA) Bronze records
    and calculate daily stock price movements.
    """

    # ==========================================
    # STEP 1: CONVERT DATA TYPES
    # ==========================================

    silver_df = (
        bronze_df
        .select(
            F.col("symbol").alias(M.SYMBOL),

            F.to_date(
                F.col("trade_date")
            ).alias(M.TRADE_DATE),

            F.col("open")
            .cast("double")
            .alias(M.OPEN),

            F.col("high")
            .cast("double")
            .alias(M.HIGH),

            F.col("low")
            .cast("double")
            .alias(M.LOW),

            F.col("close")
            .cast("double")
            .alias(M.CLOSE),

            F.col("volume")
            .cast("long")
            .alias(M.VOLUME),

            F.col("source").alias(M.SOURCE),

            F.col("batch_id").alias(M.BATCH_ID),

            F.col("ingested_at")
            .cast("timestamp")
            .alias(M.INGESTED_AT),

            F.col("bronze_loaded_at")
            .cast("timestamp")
            .alias(M.BRONZE_LOADED_AT),
        )
    )

    # ==========================================
    # STEP 2: VALIDATE STOCK PRICES
    # ==========================================

    silver_df = (
        silver_df
        .filter(
            (F.col(M.SYMBOL) == "NVDA")
            & F.col(M.TRADE_DATE).isNotNull()
            & (F.col(M.OPEN) > 0)
            & (F.col(M.HIGH) > 0)
            & (F.col(M.LOW) > 0)
            & (F.col(M.CLOSE) > 0)
            & (F.col(M.VOLUME) >= 0)
            & (F.col(M.HIGH) >= F.col(M.LOW))
            & (F.col(M.HIGH) >= F.col(M.OPEN))
            & (F.col(M.HIGH) >= F.col(M.CLOSE))
            & (F.col(M.LOW) <= F.col(M.OPEN))
            & (F.col(M.LOW) <= F.col(M.CLOSE))
        )
    )

    # ==========================================
    # STEP 3: REMOVE DUPLICATES
    # Keep the latest ingested record per date
    # ==========================================

    dedup_window = (
        Window
        .partitionBy(M.SYMBOL, M.SOURCE, M.TRADE_DATE)
        .orderBy(
            F.col(M.INGESTED_AT).desc_nulls_last(),
            F.col(M.BATCH_ID).desc_nulls_last()
        )
    )

    silver_df = (
        silver_df
        .withColumn(
            "_row_number",
            F.row_number().over(dedup_window)
        )
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )

    # ==========================================
    # STEP 4: CALCULATE DAILY PRICE CHANGE
    # ==========================================

    price_window = (
        Window
        .partitionBy(M.SYMBOL, M.SOURCE)
        .orderBy(M.TRADE_DATE)
    )

    silver_df = silver_df.withColumn(
        "_previous_close",
        F.lag(F.col(M.CLOSE)).over(price_window)
    )

    silver_df = silver_df.withColumn(
        M.DAILY_PRICE_CHANGE,
        F.round(
            F.col(M.CLOSE) - F.col("_previous_close"),
            4
        )
    )

    # ==========================================
    # STEP 5: CALCULATE DAILY RETURN %
    # ==========================================

    silver_df = silver_df.withColumn(
        M.DAILY_RETURN_PCT,
        F.when(
            F.col("_previous_close") > 0,
            F.round(
                (
                    (F.col(M.CLOSE) - F.col("_previous_close"))
                    / F.col("_previous_close")
                ) * 100,
                4
            )
        )
    )

    # ==========================================
    # STEP 6: ADD SILVER AUDIT TIMESTAMP
    # ==========================================

    return (
        silver_df
        .drop("_previous_close")
        .withColumn(
            M.SILVER_LOADED_AT,
            F.current_timestamp()
        )
        .select(*M.SILVER_COLUMNS)
    )
