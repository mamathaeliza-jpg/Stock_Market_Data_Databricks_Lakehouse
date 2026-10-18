
# ==========================================
# SILVER 5: MARKET OBSERVATIONS TEST
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

from Silver.Mapper.Market_Observations_Silver_Mapper import (
    Market_Observations_Silver_Mapper as Mapper
)

from Silver.Model.Market_Observations_Silver_Model import (
    Market_Observations_Silver_Model as Model
)


# ==========================================
# STEP 2: START SPARK
# ==========================================

spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("Silver5MarketObservationsTest")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


# ==========================================
# STEP 3: CREATE SAMPLE STOCK DATA
# ==========================================

stock_data = [
    ("AAPL", date(2026, 10, 15), 250.0, 255.0, 248.0, 253.0, 1000000.0),
    ("AAPL", date(2026, 10, 15), 250.0, 255.0, 248.0, 253.0, 1000000.0),
    ("NVDA", date(2026, 10, 15), 180.0, 185.0, 178.0, 183.0, 2000000.0)
]

stock_columns = [
    "symbol",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "volume"
]

stock_df = spark.createDataFrame(
    stock_data,
    stock_columns
)

stock_observations = Mapper.map_stock_prices(
    stock_df,
    "Alpha Vantage",
    "workspace.bronze.stock_prices"
)


# ==========================================
# STEP 4: CREATE SAMPLE FINNHUB DATA
# ==========================================

quote_df = spark.createDataFrame(
    [
        ("MSFT", "2026-10-15 12:30:00", 520.50)
    ],
    ["symbol", "quote_timestamp", "current_price"]
)

quote_observations = Mapper.map_stock_quotes(
    quote_df,
    "workspace.bronze.finnhub_quotes"
)


# ==========================================
# STEP 5: CREATE SAMPLE CRYPTO DATA
# ==========================================

crypto_df = spark.createDataFrame(
    [
        ("btc", "2026-10-15", 68000.0, 1300000000000.0, 25000000000.0),
        ("eth", "2026-10-15", 3500.0, 420000000000.0, 12000000000.0)
    ],
    [
        "symbol",
        "last_updated",
        "current_price",
        "market_cap",
        "total_volume"
    ]
)

crypto_observations = Mapper.map_crypto(
    crypto_df,
    "workspace.bronze.coingecko_prices"
)


# ==========================================
# STEP 6: CREATE SAMPLE FRED DATA
# ==========================================

fred_df = spark.createDataFrame(
    [
        ("DFF", date(2026, 10, 15), 4.25)
    ],
    ["series_id", "observation_date", "interest_rate"]
)

fred_observations = Mapper.map_interest_rates(
    fred_df,
    "workspace.bronze.fred_rates"
)


# ==========================================
# STEP 7: MERGE OBSERVATIONS
# ==========================================

combined_df = Mapper.merge_observations(
    [
        stock_observations,
        quote_observations,
        crypto_observations,
        fred_observations
    ]
)


# ==========================================
# STEP 8: CREATE IDENTIFIERS
# ==========================================

mapped_df = Mapper.map_identifiers(combined_df)

deduplicated_df = Mapper.deduplicate(mapped_df)

identified_df = Mapper.create_observation_ids(
    deduplicated_df
)

result = Mapper.map_final(identified_df).cache()


# ==========================================
# STEP 9: VALIDATE MODEL COLUMNS
# ==========================================

assert result.columns == Model.COLUMNS, (
    f"Unexpected columns: {result.columns}"
)

print("PASS: Silver model columns match")


# ==========================================
# STEP 10: VALIDATE RECORD COUNT
# ==========================================

# 2 stocks x 5 metrics = 10
# 1 Finnhub quote = 1
# 2 crypto assets x 3 metrics = 6
# 1 FRED interest rate = 1
# Total = 18 unique observations

assert result.count() == 18, (
    f"Expected 18 observations, got {result.count()}"
)

print("PASS: Expected 18 observations")


# ==========================================
# STEP 11: VALIDATE NON-NULL IDENTIFIERS
# ==========================================

null_ids = result.filter(
    F.col("observation_id").isNull()
    | F.col("asset_id").isNull()
    | F.col("source_id").isNull()
).count()

assert null_ids == 0, "Found null identifiers"

print("PASS: No null identifiers")


# ==========================================
# STEP 12: VALIDATE UNIQUE OBSERVATION IDS
# ==========================================

assert (
    result.select("observation_id").distinct().count()
    == result.count()
), "Duplicate observation IDs found"

print("PASS: Observation IDs are unique")


# ==========================================
# STEP 13: VALIDATE OBSERVATION TYPES
# ==========================================

actual_types = {
    row["observation_type"]
    for row in result.select("observation_type").distinct().collect()
}

expected_types = {
    Model.STOCK_PRICE,
    Model.STOCK_QUOTE,
    Model.CRYPTO_MARKET,
    Model.INTEREST_RATE
}

assert actual_types == expected_types, (
    f"Unexpected observation types: {actual_types}"
)

print("PASS: All four observation types present")


# ==========================================
# STEP 14: DISPLAY TEST RESULTS
# ==========================================

result.orderBy(
    "observation_type",
    "asset_id",
    "metric_type"
).show(30, truncate=False)


# ==========================================
# STEP 15: TEST COMPLETED
# ==========================================

print("ALL SILVER 5 MARKET OBSERVATIONS TESTS PASSED!")

result.unpersist()
spark.stop()
