
# ==========================================
# SILVER 4: MARKET ASSETS PROCESS
# ==========================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.Silver.Mapper.Market_Assets_Silver_Mapper import (
    Market_Assets_Silver_Mapper
)
from src.Silver.Model.Market_Assets_Silver_Model import (
    Market_Assets_Silver_Model
)


# ==========================================
# STEP 1: INITIALIZE SPARK
# ==========================================

spark = SparkSession.builder.getOrCreate()


# ==========================================
# STEP 2: DEFINE BRONZE SOURCE MAPPING
# ==========================================

BRONZE_SOURCES = [
    ("stock_prices", "AAPL", "Apple", "STOCK", "Alpha Vantage"),
    ("finnhub_quotes", "MSFT", "Microsoft", "STOCK", "Finnhub"),
    ("twelve_data_stock_prices", "NVDA", "NVIDIA", "STOCK", "Twelve Data"),
    ("fmp_stock_prices", "AMZN", "Amazon", "STOCK", "FMP"),
    ("fred_rates", "DFF", "Federal Funds Effective Rate",
     "ECONOMIC_INDICATOR", "FRED"),
    ("massive_stock_prices", "TSLA", "Tesla", "STOCK", "Massive"),
    ("marketstack_stock_prices", "NFLX", "Netflix", "STOCK", "Marketstack"),
    ("eodhd_stock_prices", "JPM", "JPMorgan Chase", "STOCK", "EODHD"),
    ("tiingo_stock_prices", "KO", "Coca-Cola", "STOCK", "Tiingo"),
]


# ==========================================
# STEP 3: READ BRONZE AND EXTRACT ASSETS
# ==========================================

asset_frames = []

for table, symbol, name, asset_type, source in BRONZE_SOURCES:

    bronze_df = spark.table(f"workspace.bronze.{table}")

    asset_df = (
        bronze_df
        .limit(1)
        .select(
            F.lit(symbol).alias("symbol"),
            F.lit(name).alias("asset_name"),
            F.lit(asset_type).alias("asset_type"),
            F.lit(source).alias("source_name")
        )
    )

    asset_frames.append(asset_df)


# ==========================================
# STEP 4: INCLUDE COINGECKO ASSETS
# ==========================================

crypto_df = (
    spark.table("workspace.bronze.coingecko_prices")
    .select(
        F.upper(F.col("symbol")).alias("symbol"),
        F.col("coin_name").alias("asset_name"),
        F.lit("CRYPTO").alias("asset_type"),
        F.lit("CoinGecko").alias("source_name")
    )
    .dropDuplicates(["symbol"])
)

asset_frames.append(crypto_df)


# ==========================================
# STEP 5: COMBINE ALL ASSET RECORDS
# ==========================================

combined_df = asset_frames[0]

for df in asset_frames[1:]:
    combined_df = combined_df.unionByName(df)


# ==========================================
# STEP 6: APPLY SILVER MAPPER
# ==========================================

mapper = Market_Assets_Silver_Mapper

silver_df = mapper.map_assets(combined_df)
silver_df = mapper.create_asset_ids(silver_df)
silver_df = mapper.create_source_ids(silver_df)

# One canonical row per asset.
silver_df = silver_df.dropDuplicates(
    ["asset_id"]
)

silver_df = mapper.map_final(silver_df)


# ==========================================
# STEP 7: CREATE SILVER SCHEMA
# ==========================================

spark.sql(
    "CREATE SCHEMA IF NOT EXISTS workspace.silver"
)


# ==========================================
# STEP 8: WRITE SILVER DELTA TABLE
# ==========================================

(
    silver_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        Market_Assets_Silver_Model.TABLE_NAME
    )
)


# ==========================================
# STEP 9: DISPLAY PROCESS RESULT
# ==========================================

print("Silver 4 Market Assets process completed.")

spark.table(
    Market_Assets_Silver_Model.TABLE_NAME
).show(truncate=False)
