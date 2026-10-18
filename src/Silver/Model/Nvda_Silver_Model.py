
# ==================================================
# TWELVE DATA SILVER MODEL - NVIDIA (NVDA)
# ==================================================

# Stock identification
SYMBOL = "symbol"
SOURCE = "source"
BATCH_ID = "batch_id"

# Trading date
TRADE_DATE = "trade_date"

# Stock prices
OPEN = "open"
HIGH = "high"
LOW = "low"
CLOSE = "close"
VOLUME = "volume"

# Additional Silver columns
DAILY_PRICE_CHANGE = "daily_price_change"
DAILY_RETURN_PCT = "daily_return_pct"

# Audit columns
INGESTED_AT = "ingested_at"
BRONZE_LOADED_AT = "bronze_loaded_at"
SILVER_LOADED_AT = "silver_loaded_at"

# Silver output columns
SILVER_COLUMNS = [
    SYMBOL,
    TRADE_DATE,
    OPEN,
    HIGH,
    LOW,
    CLOSE,
    VOLUME,
    DAILY_PRICE_CHANGE,
    DAILY_RETURN_PCT,
    SOURCE,
    BATCH_ID,
    INGESTED_AT,
    BRONZE_LOADED_AT,
    SILVER_LOADED_AT,
]
