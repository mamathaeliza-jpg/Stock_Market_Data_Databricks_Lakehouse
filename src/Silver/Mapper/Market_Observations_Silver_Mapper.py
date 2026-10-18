# ==========================================
# SILVER 5: MARKET OBSERVATIONS MAPPER
# ==========================================

from pyspark.sql import functions as F
from pyspark.sql import Window

from Silver.Model.Market_Observations_Silver_Model import (
    Market_Observations_Silver_Model as M
)


class Market_Observations_Silver_Mapper:

    # ==========================================
    # STEP 1: STANDARDIZE STOCK OBSERVATIONS
    # ==========================================

    @staticmethod
    def map_stock_prices(df, source_name, bronze_table):

        stock_df = (
            df
            .select(
                F.upper(F.trim(F.col("symbol"))).alias("symbol"),
                F.to_date("trade_date").alias("observation_date"),
                F.col("open").cast("double").alias("open"),
                F.col("high").cast("double").alias("high"),
                F.col("low").cast("double").alias("low"),
                F.col("close").cast("double").alias("close"),
                F.col("volume").cast("double").alias("volume")
            )
        )

        return Market_Observations_Silver_Mapper._to_metrics(
            stock_df,
            ["open", "high", "low", "close", "volume"],
            M.STOCK_PRICE,
            source_name,
            bronze_table
        )

    # ==========================================
    # STEP 2: STANDARDIZE FINNHUB QUOTES
    # ==========================================

    @staticmethod
    def map_stock_quotes(df, bronze_table):

        quote_df = (
            df
            .select(
                F.upper(F.trim(F.col("symbol"))).alias("symbol"),
                F.to_date(
                    F.col("quote_timestamp")
                ).alias("observation_date"),
                F.col("current_price").cast("double").alias("price")
            )
        )

        return Market_Observations_Silver_Mapper._to_metrics(
            quote_df,
            ["price"],
            M.STOCK_QUOTE,
            "Finnhub",
            bronze_table
        )

    # ==========================================
    # STEP 3: STANDARDIZE CRYPTO OBSERVATIONS
    # ==========================================

    @staticmethod
    def map_crypto(df, bronze_table):

        crypto_df = (
            df
            .select(
                F.upper(F.trim(F.col("symbol"))).alias("symbol"),
                F.to_date("last_updated").alias("observation_date"),
                F.col("current_price").cast("double").alias("price"),
                F.col("market_cap").cast("double").alias("market_cap"),
                F.col("total_volume").cast("double").alias("volume")
            )
        )

        return Market_Observations_Silver_Mapper._to_metrics(
            crypto_df,
            ["price", "market_cap", "volume"],
            M.CRYPTO_MARKET,
            "CoinGecko",
            bronze_table
        )

    # ==========================================
    # STEP 4: STANDARDIZE FRED INTEREST RATES
    # ==========================================

    @staticmethod
    def map_interest_rates(df, bronze_table):

        rates_df = (
            df
            .select(
                F.upper(F.trim(F.col("series_id"))).alias("symbol"),
                F.to_date("observation_date").alias("observation_date"),
                F.col("interest_rate").cast("double").alias("rate")
            )
        )

        return Market_Observations_Silver_Mapper._to_metrics(
            rates_df,
            ["rate"],
            M.INTEREST_RATE,
            "FRED",
            bronze_table
        )

    # ==========================================
    # STEP 5: CONVERT WIDE DATA TO METRICS
    # ==========================================

    @staticmethod
    def _to_metrics(
        df,
        metric_columns,
        observation_type,
        source_name,
        bronze_table
    ):

        metric_structs = [
            F.struct(
                F.lit(column).alias("metric_type"),
                F.col(column).cast("double").alias("metric_value")
            )
            for column in metric_columns
        ]

        return (
            df
            .withColumn(
                "metric",
                F.explode(F.array(*metric_structs))
            )
            .select(
                "symbol",
                "observation_date",
                F.lit(observation_type).alias("observation_type"),
                F.col("metric.metric_type").alias("metric_type"),
                F.col("metric.metric_value").alias("metric_value"),
                F.lit(source_name).alias("source_name"),
                F.lit(bronze_table).alias("bronze_table")
            )
        )

    # ==========================================
    # STEP 6: MERGE ALL API OBSERVATIONS
    # ==========================================

    @staticmethod
    def merge_observations(dataframes):

        if not dataframes:
            raise ValueError("No observation DataFrames provided")

        combined_df = dataframes[0]

        for df in dataframes[1:]:
            combined_df = combined_df.unionByName(df)

        return combined_df

    # ==========================================
    # STEP 7: APPLY ASSET AND SOURCE KEYS
    # ==========================================

    @staticmethod
    def map_identifiers(df):

        return (
            df
            .withColumn(
                M.ASSET_ID,
                F.sha2(
                    F.concat_ws(
                        "|",
                        F.when(
                            F.col("observation_type") == M.CRYPTO_MARKET,
                            F.lit("CRYPTO")
                        )
                        .when(
                            F.col("observation_type") == M.INTEREST_RATE,
                            F.lit("ECONOMIC_INDICATOR")
                        )
                        .otherwise(F.lit("STOCK")),
                        F.col("symbol")
                    ),
                    256
                )
            )
            .withColumn(
                M.SOURCE_ID,
                F.sha2(F.col("source_name"), 256)
            )
        )

    # ==========================================
    # STEP 8: REMOVE DUPLICATE OBSERVATIONS
    # ==========================================

    @staticmethod
    def deduplicate(df):

        window = Window.partitionBy(
            "asset_id",
            "source_id",
            "observation_date",
            "metric_type"
        ).orderBy(F.col("metric_value").desc_nulls_last())

        return (
            df
            .withColumn("row_num", F.row_number().over(window))
            .filter(F.col("row_num") == 1)
            .drop("row_num")
        )

    # ==========================================
    # STEP 9: CREATE OBSERVATION IDS
    # ==========================================

    @staticmethod
    def create_observation_ids(df):

        return df.withColumn(
            M.OBSERVATION_ID,
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

    # ==========================================
    # STEP 10: MAP FINAL SILVER COLUMNS
    # ==========================================

    @staticmethod
    def map_final(df):

        return (
            df
            .withColumn(
                M.SILVER_LOADED_AT,
                F.current_timestamp()
            )
            .select(*M.COLUMNS)
        )
