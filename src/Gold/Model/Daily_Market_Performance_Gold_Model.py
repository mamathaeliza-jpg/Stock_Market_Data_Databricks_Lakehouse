
# ==========================================
# GOLD 1: DAILY MARKET PERFORMANCE MODEL
# ==========================================

class Daily_Market_Performance_Gold_Model:

    # ==========================================
    # STEP 1: TARGET TABLE
    # ==========================================

    TABLE_NAME = "workspace.gold.daily_market_performance"

    # ==========================================
    # STEP 2: ASSET INFORMATION
    # ==========================================

    ASSET_ID = "asset_id"
    SYMBOL = "symbol"
    ASSET_NAME = "asset_name"
    ASSET_TYPE = "asset_type"

    # ==========================================
    # STEP 3: SOURCE INFORMATION
    # ==========================================

    SOURCE_ID = "source_id"
    SOURCE_NAME = "source_name"

    # ==========================================
    # STEP 4: DATE AND PRICE METRICS
    # ==========================================

    OBSERVATION_DATE = "observation_date"

    OPEN_PRICE = "open_price"
    HIGH_PRICE = "high_price"
    LOW_PRICE = "low_price"
    CLOSE_PRICE = "close_price"
    VOLUME = "volume"

    # ==========================================
    # STEP 5: PERFORMANCE METRICS
    # ==========================================

    PREVIOUS_CLOSE = "previous_close"
    PRICE_CHANGE = "price_change"
    PERCENTAGE_CHANGE = "percentage_change"
    DAILY_RANGE = "daily_range"

    # ==========================================
    # STEP 6: AUDIT INFORMATION
    # ==========================================

    GOLD_LOADED_AT = "gold_loaded_at"

    # ==========================================
    # STEP 7: FINAL COLUMNS
    # ==========================================

    COLUMNS = [
        ASSET_ID,
        SYMBOL,
        ASSET_NAME,
        ASSET_TYPE,
        SOURCE_ID,
        SOURCE_NAME,
        OBSERVATION_DATE,
        OPEN_PRICE,
        HIGH_PRICE,
        LOW_PRICE,
        CLOSE_PRICE,
        VOLUME,
        PREVIOUS_CLOSE,
        PRICE_CHANGE,
        PERCENTAGE_CHANGE,
        DAILY_RANGE,
        GOLD_LOADED_AT
    ]
