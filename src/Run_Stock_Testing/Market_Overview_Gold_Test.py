
# ==========================================
# GOLD 2: MARKET OVERVIEW TEST CASE
# ==========================================

import os
import sys

# ==========================================
# STEP 1: CONFIGURE PYSPARK PYTHON
# ==========================================

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from Gold.Mapper.Market_Overview_Gold_Mapper import (
    Market_Overview_Gold_Mapper as Mapper
)

from Gold.Model.Market_Overview_Gold_Model import (
    Market_Overview_Gold_Model as Model
)


# ==========================================
# STEP 2: INITIALIZE SPARK SESSION
# ==========================================

spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("Gold2MarketOverviewTest")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


# ==========================================
# STEP 3: CREATE SAMPLE SILVER 4 ASSETS
# ==========================================

assets_data = [
    ("asset_aapl", "STOCK"),
    ("asset_msft", "STOCK"),
    ("asset_btc", "CRYPTO"),
    ("asset_eth", "CRYPTO"),
    ("asset_dff", "ECONOMIC_INDICATOR")
]

assets_df = spark.createDataFrame(
    assets_data,
    ["asset_id", "asset_type"]
)


# ==========================================
# STEP 4: CREATE SAMPLE SILVER 5 OBSERVATIONS
# ==========================================

test_date = date(2026, 10, 15)

observations_data = [
    # Alpha Vantage - AAPL
    ("asset_aapl", "source_alpha", "Alpha Vantage",
     test_date, "close", 255.0),
    ("asset_aapl", "source_alpha", "Alpha Vantage",
     test_date, "volume", 1000000.0),

    # Finnhub - MSFT
    ("asset_msft", "source_finnhub", "Finnhub",
     test_date, "price", 520.0),

    # CoinGecko - BTC
    ("asset_btc", "source_coingecko", "CoinGecko",
     test_date, "price", 70000.0),
    ("asset_btc", "source_coingecko", "CoinGecko",
     test_date, "volume", 25000000000.0),

    # CoinGecko - ETH
    ("asset_eth", "source_coingecko", "CoinGecko",
     test_date, "price", 3500.0),
    ("asset_eth", "source_coingecko", "CoinGecko",
     test_date, "volume", 12000000000.0),

    # FRED - DFF
    ("asset_dff", "source_fred", "FRED",
     test_date, "rate", 4.25)
]

observations_columns = [
    "asset_id",
    "source_id",
    "source_name",
    "observation_date",
    "metric_type",
    "metric_value"
]

observations_df = spark.createDataFrame(
    observations_data,
    observations_columns
)


# ==========================================
# STEP 5: CREATE SAMPLE SILVER 6 INTELLIGENCE
# ==========================================

intelligence_data = [
    ("asset_aapl", "source_alpha", test_date,
     "close", 250.0, 5.0, 2.0),

    ("asset_msft", "source_finnhub", test_date,
     "price", 515.0, 5.0, 0.970874),

    ("asset_btc", "source_coingecko", test_date,
     "price", 68000.0, 2000.0, 2.941176),

    ("asset_eth", "source_coingecko", test_date,
     "price", 3600.0, -100.0, -2.777778),

    ("asset_dff", "source_fred", test_date,
     "rate", None, None, None)
]

intelligence_columns = [
    "asset_id",
    "source_id",
    "observation_date",
    "metric_type",
    "previous_close",
    "price_change",
    "percentage_change"
]

intelligence_df = spark.createDataFrame(
    intelligence_data,
    intelligence_columns
)


# ==========================================
# STEP 6: PREPARE ALL SILVER DATA
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

enriched_df = Mapper.enrich_assets(
    prepared_observations_df,
    prepared_assets_df
)


# ==========================================
# STEP 8: CREATE DAILY ASSET METRICS
# ==========================================

daily_assets_df = Mapper.create_daily_asset_metrics(
    enriched_df
)


# ==========================================
# STEP 9: CREATE DAILY INTELLIGENCE METRICS
# ==========================================

daily_intelligence_df = Mapper.create_daily_intelligence_metrics(
    prepared_intelligence_df
)


# ==========================================
# STEP 10: JOIN DAILY INTELLIGENCE
# ==========================================

combined_df = Mapper.join_intelligence(
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

result = Mapper.map_final(
    overview_df
).cache()


# ==========================================
# STEP 13: TEST MODEL COLUMNS
# ==========================================

assert result.columns == Model.COLUMNS, (
    f"Unexpected columns: {result.columns}"
)

print("PASS: Gold 2 model columns match")


# ==========================================
# STEP 14: TEST TOTAL GOLD ROWS
# ==========================================

# Alpha Vantage = 1
# Finnhub = 1
# CoinGecko = 1 (BTC + ETH grouped)
# FRED = 1
# Total = 4

total_rows = result.count()

assert total_rows == 4, (
    f"Expected 4 Gold rows, got {total_rows}"
)

print("PASS: Expected 4 Gold market overview rows")


# ==========================================
# STEP 15: TEST API SOURCES
# ==========================================

actual_sources = {
    row["source_name"]
    for row in result.select("source_name").collect()
}

expected_sources = {
    "Alpha Vantage",
    "Finnhub",
    "CoinGecko",
    "FRED"
}

assert actual_sources == expected_sources, (
    f"Unexpected sources: {actual_sources}"
)

print("PASS: All 4 sample API sources present")


# ==========================================
# STEP 16: TEST COINGECKO ASSET COUNT
# ==========================================

crypto_row = (
    result
    .filter(F.col("source_name") == "CoinGecko")
    .first()
)

assert crypto_row is not None

assert crypto_row["total_assets"] == 2
assert crypto_row["total_observations"] == 4

print("PASS: CoinGecko has 2 assets and 4 observations")


# ==========================================
# STEP 17: TEST CRYPTO MARKET PERFORMANCE
# ==========================================

assert abs(
    crypto_row["average_price"] - 36750.0
) < 0.001

assert crypto_row["positive_movements"] == 1
assert crypto_row["negative_movements"] == 1

assert abs(
    crypto_row["total_trading_volume"] - 37000000000.0
) < 0.001

print("PASS: Crypto prices, movements and volume")


# ==========================================
# STEP 18: TEST FRED INTEREST RATE
# ==========================================

fred_row = (
    result
    .filter(F.col("source_name") == "FRED")
    .first()
)

assert fred_row is not None

assert fred_row["asset_type"] == "ECONOMIC_INDICATOR"
assert abs(
    fred_row["average_interest_rate"] - 4.25
) < 0.001

assert fred_row["average_price"] is None

print("PASS: FRED economic indicator correctly aggregated")


# ==========================================
# STEP 19: TEST STOCK PERFORMANCE
# ==========================================

aapl_row = (
    result
    .filter(F.col("source_name") == "Alpha Vantage")
    .first()
)

assert aapl_row is not None

assert abs(aapl_row["average_price"] - 255.0) < 0.001
assert abs(aapl_row["average_price_change"] - 5.0) < 0.001
assert abs(aapl_row["average_percentage_change"] - 2.0) < 0.001

print("PASS: Stock price and performance metrics")


# ==========================================
# STEP 20: TEST UNIQUE GOLD RECORDS
# ==========================================

unique_count = (
    result
    .select(
        "observation_date",
        "asset_type",
        "source_id"
    )
    .distinct()
    .count()
)

assert unique_count == total_rows, (
    "Duplicate Gold overview records found"
)

print("PASS: Unique date, asset type and source records")


# ==========================================
# STEP 21: TEST NULL KEYS
# ==========================================

null_count = (
    result
    .filter(
        F.col("observation_date").isNull()
        | F.col("asset_type").isNull()
        | F.col("source_id").isNull()
    )
    .count()
)

assert null_count == 0, "Null Gold grouping keys found"

print("PASS: No null grouping keys")


# ==========================================
# STEP 22: DISPLAY GOLD 2 RESULTS
# ==========================================

result.select(
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
    "source_name"
).show(truncate=False)


# ==========================================
# STEP 23: TEST COMPLETED
# ==========================================

print("ALL GOLD 2 MARKET OVERVIEW TESTS PASSED!")

result.unpersist()
spark.stop()
