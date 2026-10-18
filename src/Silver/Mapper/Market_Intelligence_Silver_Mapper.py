
# ==========================================
# SILVER 6: MARKET INTELLIGENCE MAPPER
# ==========================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.Silver.Model.Market_Intelligence_Silver_Model import (
    Market_Intelligence_Silver_Model as Model
)


class Market_Intelligence_Silver_Mapper:

    # ==========================================
    # STEP 1: PREPARE MARKET OBSERVATIONS
    # ==========================================

    @staticmethod
    def prepare_observations(df):

        return (
            df
            .select(
                "asset_id",
                "source_id",
                "observation_date",
                "metric_type",
                F.col("metric_value").cast("double").alias(
                    "metric_value"
                ),
                "source_name"
            )
            .withColumn(
                "metric_type",
                F.lower(F.trim(F.col("metric_type")))
            )
            .withColumn(
                "observation_date",
                F.to_date("observation_date")
            )
        )

    # ==========================================
    # STEP 2: CREATE DAILY PRICE COLUMNS
    # ==========================================

    @staticmethod
    def create_daily_prices(df):

        return (
            df
            .groupBy(
                "asset_id",
                "source_id",
                "source_name",
                "observation_date"
            )
            .pivot(
                "metric_type",
                ["open", "high", "low", "close", "volume", "price"]
            )
            .agg(F.max("metric_value"))
        )

    # ==========================================
    # STEP 3: CALCULATE PREVIOUS CLOSE
    # ==========================================

    @staticmethod
    def calculate_previous_close(df):

        window = (
            Window
            .partitionBy("asset_id", "source_id")
            .orderBy("observation_date")
        )

        return (
            df
            .withColumn(
                "effective_close",
                F.coalesce(
                    F.col("close"),
                    F.col("price")
                )
            )
            .withColumn(
                "previous_close",
                F.lag("effective_close").over(window)
            )
        )

    # ==========================================
    # STEP 4: CALCULATE PRICE CHANGE
    # ==========================================

    @staticmethod
    def calculate_price_change(df):

        return (
            df
            .withColumn(
                "price_change",
                F.when(
                    F.col("effective_close").isNotNull()
                    & F.col("previous_close").isNotNull(),
                    F.col("effective_close")
                    - F.col("previous_close")
                )
                .otherwise(F.lit(None).cast("double"))
            )
        )

    # ==========================================
    # STEP 5: CALCULATE PERCENTAGE CHANGE
    # ==========================================

    @staticmethod
    def calculate_percentage_change(df):

        return (
            df
            .withColumn(
                "percentage_change",
                F.when(
                    F.col("previous_close").isNotNull()
                    & (F.col("previous_close") != 0),
                    (
                        F.col("price_change")
                        / F.col("previous_close")
                    ) * 100
                )
                .otherwise(F.lit(None).cast("double"))
            )
        )

    # ==========================================
    # STEP 6: CALCULATE DAILY TRADING RANGE
    # ==========================================

    @staticmethod
    def calculate_daily_range(df):

        return (
            df
            .withColumn(
                "daily_range",
                F.when(
                    F.col("high").isNotNull()
                    & F.col("low").isNotNull(),
                    F.col("high") - F.col("low")
                )
                .otherwise(F.lit(None).cast("double"))
            )
        )

    # ==========================================
    # STEP 7: RESTORE METRIC-LEVEL RECORDS
    # ==========================================

    @staticmethod
    def join_intelligence(df, daily_df):

        intelligence_columns = daily_df.select(
            "asset_id",
            "source_id",
            "observation_date",
            "previous_close",
            "price_change",
            "percentage_change",
            "daily_range"
        )

        return (
            df
            .join(
                intelligence_columns,
                on=[
                    "asset_id",
                    "source_id",
                    "observation_date"
                ],
                how="left"
            )
        )

    # ==========================================
    # STEP 8: CREATE INTELLIGENCE IDENTIFIERS
    # ==========================================

    @staticmethod
    def create_intelligence_ids(df):

        return (
            df
            .withColumn(
                Model.INTELLIGENCE_ID,
                F.sha2(
                    F.concat_ws(
                        "|",
                        F.col("asset_id"),
                        F.col("source_id"),
                        F.col("observation_date").cast("string"),
                        F.col("metric_type")
                    ),
                    256
                )
            )
        )

    # ==========================================
    # STEP 9: MAP FINAL SILVER COLUMNS
    # ==========================================

    @staticmethod
    def map_final(df):

        return (
            df
            .withColumn(
                Model.SILVER_LOADED_AT,
                F.current_timestamp()
            )
            .select(*Model.COLUMNS)
        )
