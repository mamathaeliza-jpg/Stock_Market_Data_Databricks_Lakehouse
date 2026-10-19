
# ==========================================
# GOLD 2: MARKET OVERVIEW MAPPER
# ==========================================

# ==========================================
# STEP 0: IMPORT REQUIRED MODULES
# ==========================================

from pyspark.sql import functions as F

from Gold.Model.Market_Overview_Gold_Model import (
    Market_Overview_Gold_Model as Model
)


class Market_Overview_Gold_Mapper:

    # ==========================================
    # STEP 1: PREPARE SILVER 4 MARKET ASSETS
    # ==========================================

    @staticmethod
    def prepare_assets(df):

        return (
            df
            .select(
                "asset_id",
                F.upper(
                    F.trim(F.col("asset_type"))
                ).alias("asset_type")
            )
            .dropDuplicates(["asset_id"])
        )

    # ==========================================
    # STEP 2: PREPARE SILVER 5 OBSERVATIONS
    # ==========================================

    @staticmethod
    def prepare_observations(df):

        return (
            df
            .select(
                "asset_id",
                "source_id",
                F.trim(
                    F.col("source_name")
                ).alias("source_name"),
                F.to_date(
                    F.col("observation_date")
                ).alias("observation_date"),
                F.lower(
                    F.trim(F.col("metric_type"))
                ).alias("metric_type"),
                F.col("metric_value")
                .cast("double")
                .alias("metric_value")
            )
        )

    # ==========================================
    # STEP 3: PREPARE SILVER 6 INTELLIGENCE
    # ==========================================

    @staticmethod
    def prepare_intelligence(df):

        return (
            df
            .select(
                "asset_id",
                "source_id",
                F.to_date(
                    F.col("observation_date")
                ).alias("observation_date"),
                F.lower(
                    F.trim(F.col("metric_type"))
                ).alias("metric_type"),
                F.col("previous_close")
                .cast("double")
                .alias("previous_close"),
                F.col("price_change")
                .cast("double")
                .alias("price_change"),
                F.col("percentage_change")
                .cast("double")
                .alias("percentage_change")
            )
        )

    # ==========================================
    # STEP 4: ENRICH OBSERVATIONS WITH ASSETS
    # ==========================================

    @staticmethod
    def enrich_assets(observations_df, assets_df):

        return (
            observations_df
            .join(
                assets_df,
                on="asset_id",
                how="left"
            )
        )

    # ==========================================
    # STEP 5: CREATE DAILY ASSET METRICS
    # ==========================================

    @staticmethod
    def create_daily_asset_metrics(df):

        return (
            df
            .groupBy(
                "asset_id",
                "source_id",
                "source_name",
                "observation_date",
                "asset_type"
            )
            .agg(
                F.count("*").alias(
                    "observation_count"
                ),

                # Closing price
                F.max(
                    F.when(
                        F.col("metric_type") == "close",
                        F.col("metric_value")
                    )
                ).alias(
                    "close_price"
                ),

                # Quote / current price
                F.max(
                    F.when(
                        F.col("metric_type") == "price",
                        F.col("metric_value")
                    )
                ).alias(
                    "quote_price"
                ),

                # Trading volume
                F.max(
                    F.when(
                        F.col("metric_type") == "volume",
                        F.col("metric_value")
                    )
                ).alias(
                    "volume"
                ),

                # FRED interest rate
                F.max(
                    F.when(
                        F.col("metric_type").isin(
                            "rate",
                            "interest_rate"
                        ),
                        F.col("metric_value")
                    )
                ).alias(
                    "interest_rate"
                )
            )
            .withColumn(
                "market_price",
                F.coalesce(
                    F.col("close_price"),
                    F.col("quote_price")
                )
            )
        )

    # ==========================================
    # STEP 6: CREATE DAILY INTELLIGENCE METRICS
    # ==========================================

    @staticmethod
    def create_daily_intelligence_metrics(df):

        return (
            df
            .groupBy(
                "asset_id",
                "source_id",
                "observation_date"
            )
            .agg(
                F.max(
                    "previous_close"
                ).alias(
                    "previous_close"
                ),

                F.max(
                    "price_change"
                ).alias(
                    "price_change"
                ),

                F.max(
                    "percentage_change"
                ).alias(
                    "percentage_change"
                )
            )
        )

    # ==========================================
    # STEP 7: JOIN DAILY MARKET INTELLIGENCE
    # ==========================================

    @staticmethod
    def join_intelligence(daily_df, intelligence_df):

        return (
            daily_df
            .join(
                intelligence_df,
                on=[
                    "asset_id",
                    "source_id",
                    "observation_date"
                ],
                how="left"
            )
        )

    # ==========================================
    # STEP 8: AGGREGATE GOLD MARKET OVERVIEW
    # ==========================================

    @staticmethod
    def aggregate_market_overview(df):

        return (
            df
            .groupBy(
                "observation_date",
                "asset_type",
                "source_id",
                "source_name"
            )
            .agg(
                # Number of distinct assets
                F.countDistinct(
                    "asset_id"
                ).alias(
                    "total_assets"
                ),

                # Total market observations
                F.sum(
                    "observation_count"
                ).cast("long").alias(
                    "total_observations"
                ),

                # Price statistics
                F.avg(
                    "market_price"
                ).alias(
                    "average_price"
                ),

                F.min(
                    "market_price"
                ).alias(
                    "minimum_price"
                ),

                F.max(
                    "market_price"
                ).alias(
                    "maximum_price"
                ),

                # Price movement statistics
                F.avg(
                    "price_change"
                ).alias(
                    "average_price_change"
                ),

                F.avg(
                    "percentage_change"
                ).alias(
                    "average_percentage_change"
                ),

                # Positive price movements
                F.sum(
                    F.when(
                        F.col("price_change") > 0,
                        1
                    ).otherwise(0)
                ).cast("long").alias(
                    "positive_movements"
                ),

                # Negative price movements
                F.sum(
                    F.when(
                        F.col("price_change") < 0,
                        1
                    ).otherwise(0)
                ).cast("long").alias(
                    "negative_movements"
                ),

                # Unchanged prices
                F.sum(
                    F.when(
                        F.col("price_change") == 0,
                        1
                    ).otherwise(0)
                ).cast("long").alias(
                    "unchanged_movements"
                ),

                # Total trading volume
                F.sum(
                    "volume"
                ).alias(
                    "total_trading_volume"
                ),

                # FRED interest rate
                F.avg(
                    "interest_rate"
                ).alias(
                    "average_interest_rate"
                )
            )
        )

    # ==========================================
    # STEP 9: MAP FINAL GOLD COLUMNS
    # ==========================================

    @staticmethod
    def map_final(df):

        return (
            df
            .withColumn(
                Model.GOLD_LOADED_AT,
                F.current_timestamp()
            )
            .select(
                *Model.COLUMNS
            )
        )
