
# ==========================================
# GOLD 1: DAILY MARKET PERFORMANCE MAPPER
# ==========================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.Gold.Model.Daily_Market_Performance_Gold_Model import (
    Daily_Market_Performance_Gold_Model as Model
)


class Daily_Market_Performance_Gold_Mapper:

    # ==========================================
    # STEP 1: PREPARE SILVER 6 INTELLIGENCE
    # ==========================================

    @staticmethod
    def prepare_intelligence(df):

        return (
            df
            .select(
                "asset_id",
                "source_id",
                "source_name",
                F.to_date("observation_date").alias(
                    "observation_date"
                ),
                F.lower(
                    F.trim(F.col("metric_type"))
                ).alias("metric_type"),
                F.col("metric_value").cast("double").alias(
                    "metric_value"
                ),
                F.col("previous_close").cast("double").alias(
                    "previous_close"
                ),
                F.col("price_change").cast("double").alias(
                    "price_change"
                ),
                F.col("percentage_change").cast("double").alias(
                    "percentage_change"
                ),
                F.col("daily_range").cast("double").alias(
                    "daily_range"
                )
            )
        )

    # ==========================================
    # STEP 2: PIVOT MARKET METRICS
    # ==========================================

    @staticmethod
    def pivot_daily_metrics(df):

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
                [
                    "open",
                    "high",
                    "low",
                    "close",
                    "price",
                    "volume"
                ]
            )
            .agg(F.max("metric_value"))
        )

    # ==========================================
    # STEP 3: PREPARE DAILY INTELLIGENCE
    # ==========================================

    @staticmethod
    def aggregate_intelligence(df):

        return (
            df
            .groupBy(
                "asset_id",
                "source_id",
                "observation_date"
            )
            .agg(
                F.max("previous_close").alias(
                    "previous_close"
                ),
                F.max("price_change").alias(
                    "price_change"
                ),
                F.max("percentage_change").alias(
                    "percentage_change"
                ),
                F.max("daily_range").alias(
                    "daily_range"
                )
            )
        )

    # ==========================================
    # STEP 4: COMBINE DAILY PRICE METRICS
    # ==========================================

    @staticmethod
    def combine_daily_metrics(prices_df, intelligence_df):

        return (
            prices_df
            .join(
                intelligence_df,
                on=[
                    "asset_id",
                    "source_id",
                    "observation_date"
                ],
                how="left"
            )
            .withColumn(
                "open_price",
                F.col("open")
            )
            .withColumn(
                "high_price",
                F.col("high")
            )
            .withColumn(
                "low_price",
                F.col("low")
            )
            .withColumn(
                "close_price",
                F.coalesce(
                    F.col("close"),
                    F.col("price")
                )
            )
            .drop(
                "open",
                "high",
                "low",
                "close",
                "price"
            )
        )

    # ==========================================
    # STEP 5: PREPARE SILVER 4 ASSET DIMENSION
    # ==========================================

    @staticmethod
    def prepare_assets(df):

        return (
            df
            .select(
                "asset_id",
                F.upper(
                    F.trim(F.col("symbol"))
                ).alias("symbol"),
                F.col("asset_name"),
                F.col("asset_type")
            )
            .dropDuplicates(["asset_id"])
        )

    # ==========================================
    # STEP 6: ENRICH WITH ASSET INFORMATION
    # ==========================================

    @staticmethod
    def enrich_assets(daily_df, assets_df):

        return (
            daily_df
            .join(
                assets_df,
                on="asset_id",
                how="left"
            )
        )

    # ==========================================
    # STEP 7: FILTER PRICE-BASED ASSETS
    # ==========================================

    @staticmethod
    def filter_price_assets(df):

        return (
            df
            .filter(
                F.col("close_price").isNotNull()
            )
        )

    # ==========================================
    # STEP 8: CREATE FINAL GOLD COLUMNS
    # ==========================================

    @staticmethod
    def map_final(df):

        return (
            df
            .withColumn(
                Model.GOLD_LOADED_AT,
                F.current_timestamp()
            )
            .select(*Model.COLUMNS)
        )
