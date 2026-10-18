
# ==========================================
# SILVER 6: MARKET INTELLIGENCE PROCESS
# ==========================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.Silver.Mapper.Market_Intelligence_Silver_Mapper import (
    Market_Intelligence_Silver_Mapper as Mapper
)

from src.Silver.Model.Market_Intelligence_Silver_Model import (
    Market_Intelligence_Silver_Model as Model
)


# ==========================================
# STEP 1: INITIALIZE SPARK SESSION
# ==========================================

spark = SparkSession.builder.getOrCreate()


# ==========================================
# STEP 2: DEFINE SOURCE AND TARGET TABLES
# ==========================================

SOURCE_TABLE = "workspace.silver.fact_market_observations"

TARGET_TABLE = Model.TABLE_NAME


# ==========================================
# STEP 3: READ SILVER 5 OBSERVATIONS
# ==========================================

observations_df = spark.table(SOURCE_TABLE)

print("Silver 5 market observations loaded.")


# ==========================================
# STEP 4: PREPARE OBSERVATIONS
# ==========================================

prepared_df = Mapper.prepare_observations(
    observations_df
)


# ==========================================
# STEP 5: CREATE DAILY MARKET PRICES
# ==========================================

daily_prices_df = Mapper.create_daily_prices(
    prepared_df
)


# ==========================================
# STEP 6: CALCULATE PREVIOUS CLOSE
# ==========================================

previous_close_df = Mapper.calculate_previous_close(
    daily_prices_df
)


# ==========================================
# STEP 7: CALCULATE PRICE CHANGE
# ==========================================

price_change_df = Mapper.calculate_price_change(
    previous_close_df
)


# ==========================================
# STEP 8: CALCULATE PERCENTAGE CHANGE
# ==========================================

percentage_change_df = Mapper.calculate_percentage_change(
    price_change_df
)


# ==========================================
# STEP 9: CALCULATE DAILY TRADING RANGE
# ==========================================

intelligence_df = Mapper.calculate_daily_range(
    percentage_change_df
)


# ==========================================
# STEP 10: JOIN INTELLIGENCE TO OBSERVATIONS
# ==========================================

enriched_df = Mapper.join_intelligence(
    prepared_df,
    intelligence_df
)


# ==========================================
# STEP 11: CREATE INTELLIGENCE IDENTIFIERS
# ==========================================

identified_df = Mapper.create_intelligence_ids(
    enriched_df
)


# ==========================================
# STEP 12: MAP FINAL SILVER COLUMNS
# ==========================================

silver_df = Mapper.map_final(
    identified_df
)


# ==========================================
# STEP 13: CREATE SILVER SCHEMA
# ==========================================

spark.sql(
    "CREATE SCHEMA IF NOT EXISTS workspace.silver"
)


# ==========================================
# STEP 14: WRITE SILVER 6 DELTA TABLE
# ==========================================

(
    silver_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)


# ==========================================
# STEP 15: DISPLAY SILVER 6 RESULTS
# ==========================================

print("Silver 6 Market Intelligence process completed.")

spark.table(TARGET_TABLE).select(
    "asset_id",
    "observation_date",
    "metric_type",
    "metric_value",
    "previous_close",
    "price_change",
    "percentage_change",
    "daily_range",
    "source_name"
).orderBy(
    F.col("observation_date").desc()
).show(30, truncate=False)


# ==========================================
# STEP 16: DISPLAY SOURCE SUMMARY
# ==========================================

spark.table(TARGET_TABLE).groupBy(
    "source_name"
).count().orderBy(
    "source_name"
).show(truncate=False)
