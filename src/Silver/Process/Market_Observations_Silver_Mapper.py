
# ==========================================
# SILVER 5: MARKET OBSERVATIONS PROCESS
# ==========================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.Silver.Mapper.Market_Observations_Silver_Mapper import (
    Market_Observations_Silver_Mapper as Mapper
)
from src.Silver.Model.Market_Observations_Silver_Model import (
    Market_Observations_Silver_Model as Model
)


# ==========================================
# STEP 1: INITIALIZE SPARK
# ==========================================

spark = SparkSession.builder.getOrCreate()


# ==========================================
# STEP 2: READ STOCK PRICE BRONZE TABLES
# ==========================================

STOCK_SOURCES = [
    ("stock_prices", "Alpha Vantage", "AAPL"),
    ("twelve_data_stock_prices", "Twelve Data", "NVDA"),
    ("fmp_stock_prices", "FMP", "AMZN"),
    ("massive_stock_prices", "Massive", "TSLA"),
    ("marketstack_stock_prices", "Marketstack", "NFLX"),
    ("eodhd_stock_prices", "EODHD", "JPM"),
    ("tiingo_stock_prices", "Tiingo", "KO"),
]

observation_frames = []

for table_name, source_name, default_symbol in STOCK_SOURCES:

    bronze_table = f"workspace.bronze.{table_name}"
    bronze_df = spark.table(bronze_table)

    # Normalize different Bronze column names before mapping.
    if "symbol" not in bronze_df.columns:
        bronze_df = bronze_df.withColumn(
            "symbol", F.lit(default_symbol)
        )

    if "trade_date" not in bronze_df.columns:
        if "date" in bronze_df.columns:
            bronze_df = bronze_df.withColumn(
                "trade_date", F.to_date("date")
            )
        elif "datetime" in bronze_df.columns:
            bronze_df = bronze_df.withColumn(
                "trade_date", F.to_date("datetime")
            )
        else:
            raise ValueError(
                f"{bronze_table}: missing trade_date/date/datetime"
            )

    stock_observations = Mapper.map_stock_prices(
        bronze_df,
        source_name,
        bronze_table
    )

    observation_frames.append(stock_observations)


# ==========================================
# STEP 3: READ FINNHUB QUOTES
# ==========================================

finnhub_table = "workspace.bronze.finnhub_quotes"
finnhub_df = spark.table(finnhub_table)

# Normalize timestamp column if needed.
if "quote_timestamp" not in finnhub_df.columns:
    if "timestamp" in finnhub_df.columns:
        finnhub_df = finnhub_df.withColumn(
            "quote_timestamp",
            F.to_timestamp("timestamp")
        )
    elif "quote_time" in finnhub_df.columns:
        finnhub_df = finnhub_df.withColumn(
            "quote_timestamp",
            F.to_timestamp("quote_time")
        )
    else:
        raise ValueError(
            f"{finnhub_table}: missing quote timestamp"
        )

# Normalize price column if needed.
if "current_price" not in finnhub_df.columns:
    if "price" in finnhub_df.columns:
        finnhub_df = finnhub_df.withColumn(
            "current_price", F.col("price")
        )
    elif "c" in finnhub_df.columns:
        finnhub_df = finnhub_df.withColumn(
            "current_price", F.col("c")
        )
    else:
        raise ValueError(
            f"{finnhub_table}: missing current price"
        )

if "symbol" not in finnhub_df.columns:
    finnhub_df = finnhub_df.withColumn(
        "symbol", F.lit("MSFT")
    )

observation_frames.append(
    Mapper.map_stock_quotes(
        finnhub_df,
        finnhub_table
    )
)


# ==========================================
# STEP 4: READ COINGECKO CRYPTO DATA
# ==========================================

coingecko_table = "workspace.bronze.coingecko_prices"
coingecko_df = spark.table(coingecko_table)

observation_frames.append(
    Mapper.map_crypto(
        coingecko_df,
        coingecko_table
    )
)


# ==========================================
# STEP 5: READ FRED INTEREST RATES
# ==========================================

fred_table = "workspace.bronze.fred_rates"
fred_df = spark.table(fred_table)

observation_frames.append(
    Mapper.map_interest_rates(
        fred_df,
        fred_table
    )
)


# ==========================================
# STEP 6: MERGE ALL 10 API OBSERVATIONS
# ==========================================

combined_df = Mapper.merge_observations(
    observation_frames
)


# ==========================================
# STEP 7: MAP ASSET AND SOURCE IDENTIFIERS
# ==========================================

mapped_df = Mapper.map_identifiers(
    combined_df
)


# ==========================================
# STEP 8: REMOVE DUPLICATES
# ==========================================

deduplicated_df = Mapper.deduplicate(
    mapped_df
)


# ==========================================
# STEP 9: CREATE OBSERVATION IDENTIFIERS
# ==========================================

identified_df = Mapper.create_observation_ids(
    deduplicated_df
)


# ==========================================
# STEP 10: MAP FINAL SILVER COLUMNS
# ==========================================

silver_df = Mapper.map_final(
    identified_df
)


# ==========================================
# STEP 11: CREATE SILVER SCHEMA
# ==========================================

spark.sql(
    "CREATE SCHEMA IF NOT EXISTS workspace.silver"
)


# ==========================================
# STEP 12: WRITE SILVER DELTA TABLE
# ==========================================

(
    silver_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(Model.TABLE_NAME)
)


# ==========================================
# STEP 13: DISPLAY PROCESS RESULTS
# ==========================================

print("Silver 5 Market Observations process completed.")

spark.table(Model.TABLE_NAME).groupBy(
    "source_name",
    "observation_type"
).count().orderBy(
    "source_name",
    "observation_type"
).show(50, truncate=False)
