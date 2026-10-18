
# ==========================================
# SILVER 4: MARKET ASSETS MAPPER
# ==========================================

from pyspark.sql import functions as F


class Market_Assets_Silver_Mapper:

    # ==========================================
    # STEP 1: STANDARDIZE ASSET DATA
    # ==========================================

    @staticmethod
    def map_assets(df):

        return (
            df
            .select(
                F.upper(F.trim(F.col("symbol"))).alias("symbol"),
                F.trim(F.col("asset_name")).alias("asset_name"),
                F.upper(F.trim(F.col("asset_type"))).alias("asset_type"),
                F.trim(F.col("source_name")).alias("source_name")
            )
            .filter(
                F.col("symbol").isNotNull()
                & (F.col("symbol") != "")
            )
        )

    # ==========================================
    # STEP 2: GENERATE ASSET IDENTIFIERS
    # ==========================================

    @staticmethod
    def create_asset_ids(df):

        return df.withColumn(
            "asset_id",
            F.sha2(
                F.concat_ws(
                    "|",
                    F.col("asset_type"),
                    F.col("symbol")
                ),
                256
            )
        )

    # ==========================================
    # STEP 3: GENERATE SOURCE IDENTIFIERS
    # ==========================================

    @staticmethod
    def create_source_ids(df):

        return df.withColumn(
            "source_id",
            F.sha2(F.col("source_name"), 256)
        )

    # ==========================================
    # STEP 4: MAP FINAL SILVER COLUMNS
    # ==========================================

    @staticmethod
    def map_final(df):

        return (
            df
            .withColumn(
                "silver_loaded_at",
                F.current_timestamp()
            )
            .select(
                "asset_id",
                "symbol",
                "asset_name",
                "asset_type",
                "source_id",
                "source_name",
                "silver_loaded_at"
            )
        )
