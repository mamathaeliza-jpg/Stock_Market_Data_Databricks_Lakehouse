
# ==========================================
# GOLD 1: DAILY MARKET PERFORMANCE PROCESS
# ==========================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.Gold.Mapper.Daily_Market_Performance_Gold_Mapper import (
    Daily_Market_Performance_Gold_Mapper as Mapper
)

from src.Gold.Model.Daily_Market_Performance_Gold_Model import (
    Daily_Market_Performance_Gold_Model as Model
)


# ==========================================
# STEP 1: INITIALIZE SPARK SESSION
# ==========================================

spark = SparkSession.builder.getOrCreate()

spark.sparkContext.setLogLevel("WARN")


# ==========================================
# STEP 2: DEFINE SOURCE AND TARGET TABLES
# ==========================================

ASSETS_TABLE = "workspace.silver.dim_market_assets"

INTELLIGENCE_TABLE = "workspace.silver.fact_market_intelligence"

TARGET_TABLE = Model.TABLE_NAME


# ==========================================
# STEP 3: READ SILVER 4 MARKET ASSETS
# ==========================================

assets_df = spark.table(ASSETS_TABLE)

print("Silver 4 Market Assets loaded.")


# ==========================================
# STEP 4: READ SILVER 6 MARKET INTELLIGENCE
# ==========================================

intelligence_df = spark.table(INTELLIGENCE_TABLE)

print("Silver 6 Market Intelligence loaded.")


# ==========================================
# STEP 5: PREPARE INTELLIGENCE DATA
# ==========================================

prepared_intelligence_df = Mapper.prepare_intelligence(
    intelligence_df
)


# ==========================================
# STEP 6: PIVOT DAILY MARKET METRICS
# ==========================================

daily_prices_df = Mapper.pivot_daily_metrics(
    prepared_intelligence_df
)


# ==========================================
# STEP 7: AGGREGATE DAILY INTELLIGENCE
# ==========================================

daily_intelligence_df = Mapper.aggregate_intelligence(
    prepared_intelligence_df
)


# ==========================================
# STEP 8: COMBINE PRICES AND INTELLIGENCE
# ==========================================

combined_df = Mapper.combine_daily_metrics(
    daily_prices_df,
    daily_intelligence_df
)


# ==========================================
# STEP 9: PREPARE ASSET DIMENSION
# ==========================================

prepared_assets_df = Mapper.prepare_assets(
    assets_df
)


# ==========================================
# STEP 10: ENRICH WITH ASSET DETAILS
# ==========================================

enriched_df = Mapper.enrich_assets(
    combined_df,
    prepared_assets_df
)


# ==========================================
# STEP 11: FILTER PRICE-BASED ASSETS
# ==========================================

price_assets_df = Mapper.filter_price_assets(
    enriched_df
)


# ==========================================
# STEP 12: MAP FINAL GOLD COLUMNS
# ==========================================

gold_df = Mapper.map_final(
    price_assets_df
)


# ==========================================
# STEP 13: CREATE GOLD SCHEMA
# ==========================================

spark.sql(
    "CREATE SCHEMA IF NOT EXISTS workspace.gold"
)


# ==========================================
# STEP 14: WRITE GOLD DELTA TABLE
# ==========================================

(
    gold_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)


# ==========================================
# STEP 15: DISPLAY GOLD RESULTS
# ==========================================

print("Gold 1 Daily Market Performance process completed.")

spark.table(TARGET_TABLE).select(
    "symbol",
    "asset_name",
    "asset_type",
    "source_name",
    "observation_date",
    "close_price",
    "previous_close",
    "price_change",
    "percentage_change",
    "daily_range",
    "volume"
).orderBy(
    F.col("observation_date").desc(),
    F.col("symbol")
).show(50, truncate=False)


# ==========================================
# STEP 16: DISPLAY SOURCE SUMMARY
# ==========================================

spark.table(TARGET_TABLE).groupBy(
    "source_name"
).count().orderBy(
    "source_name"
).show(truncate=False)
