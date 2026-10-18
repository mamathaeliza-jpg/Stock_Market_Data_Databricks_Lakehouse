
# ==========================================
# GOLD 1: DAILY MARKET PERFORMANCE TEST
# ==========================================

import os
import sys

# ==========================================
# STEP 1: CONFIGURE PYSPARK
# ==========================================

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.Gold.Mapper.Daily_Market_Performance_Gold_Mapper import (
    Daily_Market_Performance_Gold_Mapper as Mapper
)

from src.Gold.Model.Daily_Market_Performance_Gold_Model import (
    Daily_Market_Performance_Gold_Model as Model
)


# ==========================================
# STEP 2: INITIALIZE SPARK SESSION
# ==========================================

spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("Gold1DailyMarketPerformanceTest")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


# ==========================================
# STEP 3: CREATE SAMPLE SILVER 4 ASSETS
# ==========================================

assets_data = [
    ("asset_aapl", "AAPL", "Apple", "STOCK"),
    ("asset_nvda", "NVDA", "NVIDIA", "STOCK"),
    ("asset_btc", "BTC", "Bitcoin", "CRYPTO"),
    ("asset_dff", "DFF", "Federal Funds Rate", "ECONOMIC_INDICATOR")
]

assets_columns = [
    "asset_id",
    "symbol",
    "asset_name",
    "asset_type"
]

assets_df = spark.createDataFrame(
    assets_data,
    assets_columns
)


# ==========================================
# STEP 4: CREATE SAMPLE SILVER 6 INTELLIGENCE
# ==========================================

# Format:
# asset_id, source_id, source_name, date,
# metric_type, metric_value,
# previous_close, price_change,
# percentage_change, daily_range

intelligence_data = [
    # AAPL: October 14
    ("asset_aapl", "source_alpha", "Alpha Vantage",
     date(2026, 10, 14), "open", 248.0, None, None, None, 7.0),

    ("asset_aapl", "source_alpha", "Alpha Vantage",
     date(2026, 10, 14), "high", 255.0, None, None, None, 7.0),

    ("asset_aapl", "source_alpha", "Alpha Vantage",
     date(2026, 10, 14), "low", 248.0, None, None, None, 7.0),

    ("asset_aapl", "source_alpha", "Alpha Vantage",
     date(2026, 10, 14), "close", 250.0, None, None, None, 7.0),

    ("asset_aapl", "source_alpha", "Alpha Vantage",
     date(2026, 10, 14), "volume", 1000000.0, None, None, None, 7.0),

    # AAPL: October 15
    ("asset_aapl", "source_alpha", "Alpha Vantage",
     date(2026, 10, 15), "open", 251.0, 250.0, 5.0, 2.0, 8.0),

    ("asset_aapl", "source_alpha", "Alpha Vantage",
     date(2026, 10, 15), "high", 260.0, 250.0, 5.0, 2.0, 8.0),

    ("asset_aapl", "source_alpha", "Alpha Vantage",
     date(2026, 10, 15), "low", 252.0, 250.0, 5.0, 2.0, 8.0),

    ("asset_aapl", "source_alpha", "Alpha Vantage",
     date(2026, 10, 15), "close", 255.0, 250.0, 5.0, 2.0, 8.0),

    ("asset_aapl", "source_alpha", "Alpha Vantage",
     date(2026, 10, 15), "volume", 1200000.0, 250.0, 5.0, 2.0, 8.0),

    # NVDA: October 15
    ("asset_nvda", "source_twelve", "Twelve Data",
     date(2026, 10, 15), "close", 183.0, None, None, None, None),

    # BTC: October 15
    ("asset_btc", "source_coingecko", "CoinGecko",
     date(2026, 10, 15), "price", 70000.0, 68000.0,
     2000.0, 2.941176, None),

    ("asset_btc", "source_coingecko", "CoinGecko",
     date(2026, 10, 15), "volume", 25000000000.0, 68000.0,
     2000.0, 2.941176, None),

    # FRED: October 15 (must be excluded from Gold 1)
    ("asset_dff", "source_fred", "FRED",
     date(2026, 10, 15), "rate", 4.25, None, None, None, None)
]

intelligence_columns = [
    "asset_id",
    "source_id",
    "source_name",
    "observation_date",
    "metric_type",
    "metric_value",
    "previous_close",
    "price_change",
    "percentage_change",
    "daily_range"
]

intelligence_df = spark.createDataFrame(
    intelligence_data,
    intelligence_columns
)


# ==========================================
# STEP 5: PREPARE INTELLIGENCE
# ==========================================

prepared_df = Mapper.prepare_intelligence(
    intelligence_df
)


# ==========================================
# STEP 6: PIVOT DAILY METRICS
# ==========================================

daily_prices_df = Mapper.pivot_daily_metrics(
    prepared_df
)


# ==========================================
# STEP 7: AGGREGATE INTELLIGENCE
# ==========================================

daily_intelligence_df = Mapper.aggregate_intelligence(
    prepared_df
)


# ==========================================
# STEP 8: COMBINE DAILY METRICS
# ==========================================

combined_df = Mapper.combine_daily_metrics(
    daily_prices_df,
    daily_intelligence_df
)


# ==========================================
# STEP 9: PREPARE ASSETS
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

filtered_df = Mapper.filter_price_assets(
    enriched_df
)


# ==========================================
# STEP 12: MAP FINAL GOLD COLUMNS
# ==========================================

result = Mapper.map_final(
    filtered_df
).cache()


# ==========================================
# STEP 13: TEST FINAL MODEL COLUMNS
# ==========================================

assert result.columns == Model.COLUMNS, (
    f"Unexpected columns: {result.columns}"
)

print("PASS: Gold 1 model columns match")


# ==========================================
# STEP 14: TEST TOTAL GOLD ROWS
# ==========================================

# AAPL = 2 days
# NVDA = 1 day
# BTC = 1 day
# FRED = excluded
# Total = 4 rows

total_rows = result.count()

assert total_rows == 4, (
    f"Expected 4 rows, got {total_rows}"
)

print("PASS: Expected 4 daily market performance rows")


# ==========================================
# STEP 15: TEST UNIQUE DAILY RECORDS
# ==========================================

unique_rows = (
    result
    .select(
        "asset_id",
        "source_id",
        "observation_date"
    )
    .distinct()
    .count()
)

assert unique_rows == total_rows, (
    "Duplicate daily market performance records found"
)

print("PASS: One row per asset, source and date")


# ==========================================
# STEP 16: TEST AAPL PRICE CALCULATIONS
# ==========================================

aapl_day2 = (
    result
    .filter(
        (F.col("symbol") == "AAPL")
        & (F.col("observation_date") == F.lit("2026-10-15"))
    )
    .first()
)

assert aapl_day2 is not None

assert abs(aapl_day2["close_price"] - 255.0) < 0.001
assert abs(aapl_day2["previous_close"] - 250.0) < 0.001
assert abs(aapl_day2["price_change"] - 5.0) < 0.001
assert abs(aapl_day2["percentage_change"] - 2.0) < 0.001
assert abs(aapl_day2["daily_range"] - 8.0) < 0.001
assert abs(aapl_day2["volume"] - 1200000.0) < 0.001

print("PASS: AAPL price and performance metrics")


# ==========================================
# STEP 17: TEST CRYPTO PRICE MAPPING
# ==========================================

btc_row = (
    result
    .filter(F.col("symbol") == "BTC")
    .first()
)

assert btc_row is not None
assert abs(btc_row["close_price"] - 70000.0) < 0.001
assert abs(btc_row["price_change"] - 2000.0) < 0.001

print("PASS: BTC price mapped to close_price")


# ==========================================
# STEP 18: TEST ECONOMIC INDICATOR EXCLUSION
# ==========================================

fred_count = (
    result
    .filter(F.col("symbol") == "DFF")
    .count()
)

assert fred_count == 0, (
    "FRED interest rate should not appear in Gold 1"
)

print("PASS: FRED interest rate excluded")


# ==========================================
# STEP 19: TEST NULL IDENTIFIERS
# ==========================================

null_ids = (
    result
    .filter(
        F.col("asset_id").isNull()
        | F.col("source_id").isNull()
        | F.col("symbol").isNull()
    )
    .count()
)

assert null_ids == 0, "Null asset or source identifiers found"

print("PASS: No null asset or source identifiers")


# ==========================================
# STEP 20: DISPLAY GOLD RESULTS
# ==========================================

result.select(
    "symbol",
    "asset_name",
    "source_name",
    "observation_date",
    "close_price",
    "previous_close",
    "price_change",
    "percentage_change",
    "daily_range",
    "volume"
).orderBy(
    "symbol",
    "observation_date"
).show(truncate=False)


# ==========================================
# STEP 21: TEST COMPLETED
# ==========================================

print("ALL GOLD 1 DAILY MARKET PERFORMANCE TESTS PASSED!")

result.unpersist()
spark.stop()
