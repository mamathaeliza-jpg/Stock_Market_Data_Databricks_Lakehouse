# ==========================================
# SILVER 6: MARKET INTELLIGENCE TEST
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

from src.Silver.Mapper.Market_Intelligence_Silver_Mapper import (
     Market_Intelligence_Silver_Mapper as Mapper
)

from src.Silver.Model.Market_Intelligence_Silver_Model import (
     Market_Intelligence_Silver_Model as Model
)


# ==========================================
# STEP 2: START SPARK
# ==========================================

spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("Silver6MarketIntelligenceTest")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


# ==========================================
# STEP 3: CREATE SAMPLE SILVER 5 DATA
# ==========================================

sample_data = [
    # AAPL - Day 1
    ("stock_aapl", "source_alpha", date(2026, 10, 14),
     "close", 250.0, "Alpha Vantage"),
    ("stock_aapl", "source_alpha", date(2026, 10, 14),
     "high", 255.0, "Alpha Vantage"),
    ("stock_aapl", "source_alpha", date(2026, 10, 14),
     "low", 248.0, "Alpha Vantage"),

    # AAPL - Day 2
    ("stock_aapl", "source_alpha", date(2026, 10, 15),
     "close", 255.0, "Alpha Vantage"),
    ("stock_aapl", "source_alpha", date(2026, 10, 15),
     "high", 260.0, "Alpha Vantage"),
    ("stock_aapl", "source_alpha", date(2026, 10, 15),
     "low", 252.0, "Alpha Vantage"),

    # AAPL - Day 3
    ("stock_aapl", "source_alpha", date(2026, 10, 16),
     "close", 252.0, "Alpha Vantage"),
    ("stock_aapl", "source_alpha", date(2026, 10, 16),
     "high", 257.0, "Alpha Vantage"),
    ("stock_aapl", "source_alpha", date(2026, 10, 16),
     "low", 249.0, "Alpha Vantage"),

    # BTC - Day 1 and Day 2
    ("crypto_btc", "source_coingecko", date(2026, 10, 14),
     "price", 68000.0, "CoinGecko"),
    ("crypto_btc", "source_coingecko", date(2026, 10, 15),
     "price", 70000.0, "CoinGecko"),

    # FRED interest rate
    ("economic_dff", "source_fred", date(2026, 10, 15),
     "rate", 4.25, "FRED")
]

columns = [
    "asset_id",
    "source_id",
    "observation_date",
    "metric_type",
    "metric_value",
    "source_name"
]

observations_df = spark.createDataFrame(
    sample_data,
    columns
)


# ==========================================
# STEP 4: PREPARE OBSERVATIONS
# ==========================================

prepared_df = Mapper.prepare_observations(
    observations_df
)


# ==========================================
# STEP 5: CREATE DAILY PRICES
# ==========================================

daily_df = Mapper.create_daily_prices(
    prepared_df
)


# ==========================================
# STEP 6: CALCULATE PREVIOUS CLOSE
# ==========================================

daily_df = Mapper.calculate_previous_close(
    daily_df
)


# ==========================================
# STEP 7: CALCULATE PRICE CHANGE
# ==========================================

daily_df = Mapper.calculate_price_change(
    daily_df
)


# ==========================================
# STEP 8: CALCULATE PERCENTAGE CHANGE
# ==========================================

daily_df = Mapper.calculate_percentage_change(
    daily_df
)


# ==========================================
# STEP 9: CALCULATE DAILY RANGE
# ==========================================

daily_df = Mapper.calculate_daily_range(
    daily_df
)


# ==========================================
# STEP 10: JOIN INTELLIGENCE
# ==========================================

enriched_df = Mapper.join_intelligence(
    prepared_df,
    daily_df
)


# ==========================================
# STEP 11: CREATE IDS AND FINAL COLUMNS
# ==========================================

identified_df = Mapper.create_intelligence_ids(
    enriched_df
)

result = Mapper.map_final(
    identified_df
).cache()


# ==========================================
# STEP 12: TEST MODEL COLUMNS
# ==========================================

assert result.columns == Model.COLUMNS, (
    f"Unexpected columns: {result.columns}"
)

print("PASS: Silver 6 model columns match")


# ==========================================
# STEP 13: TEST RECORD COUNT
# ==========================================

assert result.count() == 12, (
    f"Expected 12 rows, got {result.count()}"
)

print("PASS: Expected 12 market intelligence rows")


# ==========================================
# STEP 14: TEST UNIQUE INTELLIGENCE IDS
# ==========================================

assert (
    result.select("intelligence_id").distinct().count()
    == result.count()
), "Duplicate intelligence IDs found"

print("PASS: Intelligence IDs are unique")


# ==========================================
# STEP 15: TEST NULL IDENTIFIERS
# ==========================================

null_count = result.filter(
    F.col("intelligence_id").isNull()
    | F.col("asset_id").isNull()
    | F.col("source_id").isNull()
).count()

assert null_count == 0, "Null identifiers found"

print("PASS: No null identifiers")


# ==========================================
# STEP 16: TEST AAPL PRICE CALCULATIONS
# ==========================================

aapl_day2 = (
    result
    .filter(
        (F.col("asset_id") == "stock_aapl")
        & (F.col("observation_date") == F.lit("2026-10-15"))
        & (F.col("metric_type") == "close")
    )
    .first()
)

assert aapl_day2 is not None

assert abs(aapl_day2["previous_close"] - 250.0) < 0.001
assert abs(aapl_day2["price_change"] - 5.0) < 0.001
assert abs(aapl_day2["percentage_change"] - 2.0) < 0.001
assert abs(aapl_day2["daily_range"] - 8.0) < 0.001

print("PASS: AAPL price change, percentage and range")


# ==========================================
# STEP 17: TEST NEGATIVE PRICE CHANGE
# ==========================================

aapl_day3 = (
    result
    .filter(
        (F.col("asset_id") == "stock_aapl")
        & (F.col("observation_date") == F.lit("2026-10-16"))
        & (F.col("metric_type") == "close")
    )
    .first()
)

assert aapl_day3 is not None

assert abs(aapl_day3["price_change"] - (-3.0)) < 0.001

expected_percentage = (-3.0 / 255.0) * 100

assert abs(
    aapl_day3["percentage_change"] - expected_percentage
) < 0.001

print("PASS: Negative price change calculated correctly")


# ==========================================
# STEP 18: TEST CRYPTO PRICE CHANGE
# ==========================================

btc_day2 = (
    result
    .filter(
        (F.col("asset_id") == "crypto_btc")
        & (F.col("observation_date") == F.lit("2026-10-15"))
    )
    .first()
)

assert btc_day2 is not None
assert abs(btc_day2["price_change"] - 2000.0) < 0.001

print("PASS: BTC price change calculated correctly")


# ==========================================
# STEP 19: TEST FRED INTEREST RATE
# ==========================================

fred_row = (
    result
    .filter(F.col("asset_id") == "economic_dff")
    .first()
)

assert fred_row is not None
assert fred_row["previous_close"] is None
assert fred_row["price_change"] is None
assert fred_row["daily_range"] is None

print("PASS: FRED rate has no price-based calculations")


# ==========================================
# STEP 20: DISPLAY TEST RESULTS
# ==========================================

result.select(
    "asset_id",
    "observation_date",
    "metric_type",
    "metric_value",
    "previous_close",
    "price_change",
    "percentage_change",
    "daily_range"
).orderBy(
    "asset_id",
    "observation_date",
    "metric_type"
).show(30, truncate=False)


# ==========================================
# STEP 21: TEST COMPLETED
# ==========================================

print("ALL SILVER 6 MARKET INTELLIGENCE TESTS PASSED!")

result.unpersist()
spark.stop()
