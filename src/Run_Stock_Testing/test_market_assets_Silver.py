
# ==========================================
# CONFIGURE LOCAL PYSPARK ON WINDOWS
# ==========================================

import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# ==========================================
# SILVER 4: MARKET ASSETS TEST
# ==========================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from Silver.Model.Market_Assets_Silver_Model import (
    Market_Assets_Silver_Model
)
from Silver.Mapper.Market_Assets_Silver_Mapper import (
    Market_Assets_Silver_Mapper
)


# ==========================================
# STEP 1: INITIALIZE SPARK
# ==========================================

spark = SparkSession.builder.getOrCreate()


# ==========================================
# STEP 2: CREATE SAMPLE DATA
# ==========================================

test_data = [
    ("aapl", "Apple", "stock", "Alpha Vantage"),
    ("MSFT", "Microsoft", "STOCK", "Finnhub"),
    ("btc", "Bitcoin", "crypto", "CoinGecko"),
    ("AAPL", "Apple", "STOCK", "Alpha Vantage")
]

columns = [
    "symbol",
    "asset_name",
    "asset_type",
    "source_name"
]

df = spark.createDataFrame(test_data, columns)


# ==========================================
# STEP 3: APPLY MAPPER
# ==========================================

mapper = Market_Assets_Silver_Mapper

result = mapper.map_assets(df)
result = mapper.create_asset_ids(result)
result = mapper.create_source_ids(result)
#result = result.dropDuplicates(["asset_id"])
result = mapper.map_final(result)


# ==========================================
# STEP 4: TEST MODEL COLUMNS
# ==========================================

assert result.columns == Market_Assets_Silver_Model.COLUMNS


# ==========================================
# STEP 5: TEST DUPLICATE REMOVAL
# ==========================================

assert result.count() == 3


# ==========================================
# STEP 6: TEST NULL VALUES
# ==========================================

assert result.filter(
    F.col("asset_id").isNull()
    | F.col("symbol").isNull()
    | F.col("source_id").isNull()
).count() == 0


# ==========================================
# STEP 7: DISPLAY RESULTS
# ==========================================

result.show(truncate=False)

print("ALL SILVER 4 MARKET ASSETS TESTS PASSED!")
