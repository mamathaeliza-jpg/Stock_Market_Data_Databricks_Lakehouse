
# ==========================================
# GOLD 2: MARKET OVERVIEW PROCESS
# ==========================================

# ==========================================
# STEP 0: CONFIGURE PYTHON IMPORT PATH
# ==========================================

import sys
from pathlib import Path

# Databricks serverless may execute Python files
# without defining the __file__ variable.

SRC_DIR = None

# Try locating src using __file__ when available.
if "__file__" in globals():

    candidate = Path(__file__).resolve().parents[2]

    if (candidate / "Gold" / "Mapper").is_dir():
        SRC_DIR = candidate

# Search the current working directory and parents.
if SRC_DIR is None:

    current_dir = Path.cwd().resolve()

    for parent in [current_dir, *current_dir.parents]:

        candidate = parent / "src"

        if (candidate / "Gold" / "Mapper").is_dir():
            SRC_DIR = candidate
            break

# Fallback for your Databricks Asset Bundle deployment.
if SRC_DIR is None:

    candidate = Path(
        "/Workspace/Users/mamathaeliza@gmail.com/"
        ".bundle/stock_market_lakehouse/dev/files/src"
    )

    if (candidate / "Gold" / "Mapper").is_dir():
        SRC_DIR = candidate

# Stop with a clear error if the source directory is missing.
if SRC_DIR is None:

    raise FileNotFoundError(
        "Cannot locate src/Gold/Mapper. "
        "Verify that the Databricks bundle deployed "
        "the Model and Mapper Python files."
    )

# Add src to Python's module search path.
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

print(f"Python source directory: {SRC_DIR}")


# ==========================================
# STEP 1: IMPORT PYSPARK
# ==========================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ==========================================
# STEP 2: IMPORT GOLD 2 MAPPER AND MODEL
# ==========================================

from Gold.Mapper.Market_Overview_Gold_Mapper import (
    Market_Overview_Gold_Mapper as Mapper
)

from Gold.Model.Market_Overview_Gold_Model import (
    Market_Overview_Gold_Model as Model
)

print("Gold 2 Mapper and Model imported successfully.")


# ==========================================
# STEP 3: INITIALIZE SPARK SESSION
# ==========================================

spark = SparkSession.builder.getOrCreate()

print("Spark session initialized successfully.")


# ==========================================
# STEP 4: DEFINE SOURCE AND TARGET TABLES
# ==========================================

ASSETS_TABLE = "workspace.silver.dim_market_assets"

OBSERVATIONS_TABLE = "workspace.silver.fact_market_observations"

INTELLIGENCE_TABLE = "workspace.silver.fact_market_intelligence"

TARGET_TABLE = Model.TABLE_NAME

print(f"Assets table: {ASSETS_TABLE}")
print(f"Observations table: {OBSERVATIONS_TABLE}")
print(f"Intelligence table: {INTELLIGENCE_TABLE}")
print(f"Target Gold table: {TARGET_TABLE}")


# ==========================================
# STEP 5: READ SILVER 4 MARKET ASSETS
# ==========================================

assets_df = spark.table(ASSETS_TABLE)

print("Silver 4 Market Assets loaded successfully.")


# ==========================================
# STEP 6: READ SILVER 5 OBSERVATIONS
# ==========================================

observations_df = spark.table(OBSERVATIONS_TABLE)

print("Silver 5 Market Observations loaded successfully.")


# ==========================================
# STEP 7: READ SILVER 6 INTELLIGENCE
# ==========================================

intelligence_df = spark.table(INTELLIGENCE_TABLE)

print("Silver 6 Market Intelligence loaded successfully.")


# ==========================================
# STEP 8: PREPARE SILVER DATA
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

print("Silver data preparation completed.")


# ==========================================
# STEP 9: ENRICH OBSERVATIONS WITH ASSETS
# ==========================================

enriched_observations_df = Mapper.enrich_assets(
    prepared_observations_df,
    prepared_assets_df
)

print("Observations enriched with asset details.")


# ==========================================
# STEP 10: CREATE DAILY ASSET METRICS
# ==========================================

daily_assets_df = Mapper.create_daily_asset_metrics(
    enriched_observations_df
)

print("Daily asset metrics created.")


# ==========================================
# STEP 11: CREATE DAILY INTELLIGENCE METRICS
# ==========================================

daily_intelligence_df = Mapper.create_daily_intelligence_metrics(
    prepared_intelligence_df
)

print("Daily intelligence metrics created.")


# ==========================================
# STEP 12: COMBINE MARKET AND INTELLIGENCE
# ==========================================

combined_df = Mapper.join_intelligence(
    daily_assets_df,
    daily_intelligence_df
)

print("Daily market and intelligence data combined.")


# ==========================================
# STEP 13: AGGREGATE MARKET OVERVIEW
# ==========================================

overview_df = Mapper.aggregate_market_overview(
    combined_df
)

print("Market overview aggregation completed.")


# ==========================================
# STEP 14: MAP FINAL GOLD COLUMNS
# ==========================================

gold_df = Mapper.map_final(
    overview_df
)

print("Gold 2 final columns mapped.")


# ==========================================
# STEP 15: VALIDATE GOLD 2 DATA
# ==========================================

total_records = gold_df.count()

print(f"Total Gold 2 records prepared: {total_records}")

if total_records == 0:

    raise ValueError(
        "Gold 2 transformation produced no records. "
        "Check the Silver source tables."
    )


# ==========================================
# STEP 16: CREATE GOLD SCHEMA
# ==========================================

spark.sql(
    "CREATE SCHEMA IF NOT EXISTS workspace.gold"
)

print("Gold schema verified successfully.")


# ==========================================
# STEP 17: WRITE GOLD 2 DELTA TABLE
# ==========================================

(
    gold_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

print(f"Gold 2 Delta table created: {TARGET_TABLE}")


# ==========================================
# STEP 18: DISPLAY GOLD 2 RESULTS
# ==========================================

print("Displaying Gold 2 Market Overview results:")

(
    spark.table(TARGET_TABLE)
    .select(
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
    )
    .orderBy(
        F.col("observation_date").desc(),
        F.col("source_name")
    )
    .show(50, truncate=False)
)


# ==========================================
# STEP 19: DISPLAY API SOURCE SUMMARY
# ==========================================

print("Displaying API source summary:")

(
    spark.table(TARGET_TABLE)
    .groupBy(
        "source_name",
        "asset_type"
    )
    .agg(
        F.sum("total_observations").alias(
            "total_observations"
        ),
        F.countDistinct("observation_date").alias(
            "total_dates"
        )
    )
    .orderBy("source_name")
    .show(truncate=False)
)


# ==========================================
# STEP 20: VERIFY GOLD 2 TABLE RECORD COUNT
# ==========================================

saved_records = spark.table(TARGET_TABLE).count()

print(f"Total Gold 2 records saved: {saved_records}")

if saved_records != total_records:

    raise ValueError(
        "Gold 2 saved record count does not match "
        "the transformed record count."
    )


# ==========================================
# STEP 21: PROCESS COMPLETED
# ==========================================

print(
    "GOLD 2 MARKET OVERVIEW PROCESS COMPLETED SUCCESSFULLY!"
)
