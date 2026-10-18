
# ==================================================
# FINNHUB SILVER MODEL - MICROSOFT (MSFT)
# ==================================================

# Stock identification
SYMBOL = "symbol"
SOURCE = "source"
BATCH_ID = "batch_id"

# Stock quote date and time
TRADE_DATE = "trade_date"
QUOTE_TIMESTAMP = "quote_timestamp"

# Stock prices
CURRENT_PRICE = "current_price"
OPEN_PRICE = "open_price"
HIGH_PRICE = "high_price"
LOW_PRICE = "low_price"
PREVIOUS_CLOSE = "previous_close"

# Price movements
PRICE_CHANGE = "price_change"
PERCENT_CHANGE = "percent_change"

# Audit columns
INGESTED_AT = "ingested_at"
BRONZE_LOADED_AT = "bronze_loaded_at"
SILVER_LOADED_AT = "silver_loaded_at"

# Silver output columns
SILVER_COLUMNS = [
    SYMBOL,
    TRADE_DATE,
    QUOTE_TIMESTAMP,
    CURRENT_PRICE,
    OPEN_PRICE,
    HIGH_PRICE,
    LOW_PRICE,
    PREVIOUS_CLOSE,
    PRICE_CHANGE,
    PERCENT_CHANGE,
    SOURCE,
    BATCH_ID,
    INGESTED_AT,
    BRONZE_LOADED_AT,
    SILVER_LOADED_AT,
]
