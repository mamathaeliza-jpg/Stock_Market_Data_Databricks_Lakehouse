
# ==========================================
# GOLD 2: MARKET OVERVIEW PROCESS
# ==========================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from Gold.Mapper.Market_Overview_Gold_Mapper import (
    Market_Overview_Gold_Mapper as Mapper
)

from Gold.Model.Market_Overview_Gold_Model import (
    Market_Overview_Gold_Model as Model
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

OBSERVATIONS_TABLE = "workspace.silver.fact_market_observations"

INTELLIGENCE_TABLE = "workspace.silver.fact_market_intelligence"

TARGET_TABLE = Model.TABLE_NAME


# ==========================================
# STEP 3: READ SILVER 4 MARKET ASSETS
# ==========================================

assets_df = spark.table(ASSETS_TABLE)

print("Silver 4 Market Assets loaded.")


# ==========================================
# STEP 4: READ SILVER 5 OBSERVATIONS
# ==========================================

observations_df = spark.table(OBSERVATIONS_TABLE)

print("Silver 5 Market Observations loaded.")


# ==========================================
# STEP 5: READ SILVER 6 INTELLIGENCE
# ==========================================

intelligence_df = spark.table(INTELLIGENCE_TABLE)

print("Silver 6 Market Intelligence loaded.")


# ==========================================
# STEP 6: PREPARE SILVER DATA
# ==========================================

prepared_assets_df = Mapper.prepare_assets(
    assets_df
)

prepared_observations_df = Mapper.prepare_observations(
    observations_df
)

prepared_intelligence_df = Mapper.prepare_intelligence(
    intelligence_df
)


# ==========================================
# STEP 7: ENRICH OBSERVATIONS WITH ASSETS
# ==========================================

enriched_observations_df = Mapper.enrich_assets(
    prepared_observations_df,
    prepared_assets_df
)


# ==========================================
# STEP 8: CREATE DAILY ASSET METRICS
# ==========================================

daily_assets_df = Mapper.create_daily_asset_metrics(
    enriched_observations_df
)


# ==========================================
# STEP 9: CREATE DAILY INTELLIGENCE METRICS
# ==========================================

daily_intelligence_df = Mapper.create_daily_intelligence_metrics(
    prepared_intelligence_df
)


# ==========================================
# STEP 10: COMBINE MARKET AND INTELLIGENCE
# ==========================================

combined_df = Mapper.combine_daily_metrics(
    daily_assets_df,
    daily_intelligence_df
)


# ==========================================
# STEP 11: AGGREGATE MARKET OVERVIEW
# ==========================================

overview_df = Mapper.aggregate_market_overview(
    combined_df
)


# ==========================================
# STEP 12: MAP FINAL GOLD COLUMNS
# ==========================================

gold_df = Mapper.map_final(
    overview_df
)


# ==========================================
# STEP 13: CREATE GOLD SCHEMA
# ==========================================

spark.sql(
    "CREATE SCHEMA IF NOT EXISTS workspace.gold"
)


# ==========================================
# STEP 14: WRITE GOLD 2 DELTA TABLE
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
# STEP 15: DISPLAY GOLD 2 RESULTS
# ==========================================

print("Gold 2 Market Overview process completed.")

spark.table(TARGET_TABLE).select(
    "observation_date",
    "asset_type",
    "source_name",
    "total_assets",
    "total_observations",
    "average_price",
    "average_percentage_change",
    "positive_movements",
    "negative_movements",
    "total_trading_volume",
    "average_interest_rate"
).orderBy(
    F.col("observation_date").desc(),
    F.col("source_name")
).show(50, truncate=False)


# ==========================================
# STEP 16: DISPLAY API SOURCE SUMMARY
# ==========================================

spark.table(TARGET_TABLE).groupBy(
    "source_name",
    "asset_type"
).agg(
    F.sum("total_observations").alias("total_observations"),
    F.countDistinct("observation_date").alias("total_dates")
).orderBy(
    "source_name"
).show(truncate=False)


# ==========================================
# STEP 17: PROCESS COMPLETED
# ==========================================

print(
    "GOLD 2 MARKET OVERVIEW PROCESS COMPLETED SUCCESSFULLY!"
)
