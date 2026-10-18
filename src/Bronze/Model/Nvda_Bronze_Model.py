
# ==================================================
# TWELVE DATA BRONZE MODEL - NVIDIA (NVDA)
# ==================================================

# Stock identification
SYMBOL = "symbol"
SOURCE = "source"
BATCH_ID = "batch_id"
INGESTED_AT = "ingested_at"

# Historical stock prices
TRADE_DATE = "trade_date"
OPEN = "open"
HIGH = "high"
LOW = "low"
CLOSE = "close"
VOLUME = "volume"

# Bronze audit field
BRONZE_LOADED_AT = "bronze_loaded_at"

# All Bronze output columns
BRONZE_COLUMNS = [
    SYMBOL,
    TRADE_DATE,
    OPEN,
    HIGH,
    LOW,
    CLOSE,
    VOLUME,
    SOURCE,
    BATCH_ID,
    INGESTED_AT,
    BRONZE_LOADED_AT,
]
