
# ==================================================
# FINNHUB BRONZE MODEL - MICROSOFT (MSFT)
# ==================================================

# Stock identification
SYMBOL = "symbol"
SOURCE = "source"
BATCH_ID = "batch_id"
INGESTED_AT = "ingested_at"

# Finnhub quote fields
QUOTE_TIMESTAMP = "quote_timestamp"
CURRENT_PRICE = "current_price"
OPEN_PRICE = "open_price"
HIGH_PRICE = "high_price"
LOW_PRICE = "low_price"
PREVIOUS_CLOSE = "previous_close"
PRICE_CHANGE = "price_change"
PERCENT_CHANGE = "percent_change"

# Bronze audit field
BRONZE_LOADED_AT = "bronze_loaded_at"

# All Bronze output columns
BRONZE_COLUMNS = [
    SYMBOL,
    SOURCE,
    BATCH_ID,
    INGESTED_AT,
    QUOTE_TIMESTAMP,
    CURRENT_PRICE,
    OPEN_PRICE,
    HIGH_PRICE,
    LOW_PRICE,
    PREVIOUS_CLOSE,
    PRICE_CHANGE,
    PERCENT_CHANGE,
    BRONZE_LOADED_AT,
]
