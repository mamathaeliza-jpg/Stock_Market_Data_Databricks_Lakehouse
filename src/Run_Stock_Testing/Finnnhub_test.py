
import sys
from pathlib import Path

from pyspark.sql import SparkSession

# ==================================================
# STEP 1: SET PROJECT SOURCE PATH
# ==================================================

try:
    SRC_DIR = Path(__file__).resolve().parents[1]
except NameError:
    SRC_DIR = Path(
        "/Workspace/Users/mamathaeliza@gmail.com/"
        ".bundle/stock_market_lakehouse/dev/files/src"
    )

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))


# ==================================================
# STEP 2: IMPORT FINNHUB PROCESSES
# ==================================================

from Bronze.Process.Finnhub_Bronze_Process import (
    process as bronze_process
)

from Silver.Process.Finnhub_Silver_Process import (
    process as silver_process
)


# ==================================================
# STEP 3: CREATE SPARK SESSION
# ==================================================

spark = SparkSession.builder.getOrCreate()


# ==================================================
# STEP 4: READ FINNHUB MICROSOFT JSON
# ==================================================

RAW_PATH = (
    "/Volumes/workspace/default/"
    "stock_market_raw/FINNHUB_MSFT_*.json"
)

raw_df = (
    spark.read
    .option("multiLine", "true")
    .json(RAW_PATH)
)

print("========== RAW FINNHUB DATA ==========")

raw_df.printSchema()
raw_df.show(truncate=False)


# ==================================================
# STEP 5: BRONZE PROCESS
# ==================================================

print("========== BRONZE PROCESS ==========")

bronze_df = bronze_process(spark, raw_df)

bronze_df.printSchema()
bronze_df.show(truncate=False)

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.bronze")

bronze_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.bronze.finnhub_quotes")

print("Bronze table saved successfully!")


# ==================================================
# STEP 6: SILVER PROCESS
# ==================================================

print("========== SILVER PROCESS ==========")

silver_df = silver_process(spark, bronze_df)

silver_df.printSchema()
silver_df.show(truncate=False)

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.silver")

silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.silver.finnhub_quotes")

print("Silver table saved successfully!")


# ==================================================
# STEP 7: VERIFY TABLES
# ==================================================

print("========== VERIFY BRONZE TABLE ==========")

spark.sql(
    """
    SELECT *
    FROM workspace.bronze.finnhub_quotes
    """
).show(truncate=False)


print("========== VERIFY SILVER TABLE ==========")

spark.sql(
    """
    SELECT *
    FROM workspace.silver.finnhub_quotes
    """
).show(truncate=False)


print("========== ROW COUNTS ==========")

print("Bronze rows:", bronze_df.count())
print("Silver rows:", silver_df.count())

print("FINNHUB MSFT BRONZE AND SILVER TEST COMPLETED!")
