
# ==========================================
# GOLD 2: MARKET OVERVIEW MODEL
# ==========================================

class Market_Overview_Gold_Model:

    # ==========================================
    # STEP 1: TARGET GOLD TABLE
    # ==========================================

    TABLE_NAME = "workspace.gold.market_overview"

    # ==========================================
    # STEP 2: DATE
    # ==========================================

    OBSERVATION_DATE = "observation_date"

    # ==========================================
    # STEP 3: MARKET CATEGORY
    # ==========================================

    ASSET_TYPE = "asset_type"

    # ==========================================
    # STEP 4: DATA SOURCE INFORMATION
    # ==========================================

    SOURCE_ID = "source_id"
    SOURCE_NAME = "source_name"

    # ==========================================
    # STEP 5: MARKET COVERAGE METRICS
    # ==========================================

    TOTAL_ASSETS = "total_assets"
    TOTAL_OBSERVATIONS = "total_observations"

    # ==========================================
    # STEP 6: MARKET PRICE METRICS
    # ==========================================

    AVERAGE_PRICE = "average_price"
    MINIMUM_PRICE = "minimum_price"
    MAXIMUM_PRICE = "maximum_price"

    # ==========================================
    # STEP 7: MARKET PERFORMANCE METRICS
    # ==========================================

    AVERAGE_PRICE_CHANGE = "average_price_change"
    AVERAGE_PERCENTAGE_CHANGE = "average_percentage_change"

    POSITIVE_MOVEMENTS = "positive_movements"
    NEGATIVE_MOVEMENTS = "negative_movements"
    UNCHANGED_MOVEMENTS = "unchanged_movements"

    # ==========================================
    # STEP 8: MARKET ACTIVITY
    # ==========================================

    TOTAL_TRADING_VOLUME = "total_trading_volume"

    # ==========================================
    # STEP 9: ECONOMIC INDICATORS
    # ==========================================

    AVERAGE_INTEREST_RATE = "average_interest_rate"

    # ==========================================
    # STEP 10: AUDIT INFORMATION
    # ==========================================

    GOLD_LOADED_AT = "gold_loaded_at"

    # ==========================================
    # STEP 11: FINAL MODEL COLUMNS
    # ==========================================

    COLUMNS = [
        OBSERVATION_DATE,
        ASSET_TYPE,
        SOURCE_ID,
        SOURCE_NAME,
        TOTAL_ASSETS,
        TOTAL_OBSERVATIONS,
        AVERAGE_PRICE,
        MINIMUM_PRICE,
        MAXIMUM_PRICE,
        AVERAGE_PRICE_CHANGE,
        AVERAGE_PERCENTAGE_CHANGE,
        POSITIVE_MOVEMENTS,
        NEGATIVE_MOVEMENTS,
        UNCHANGED_MOVEMENTS,
        TOTAL_TRADING_VOLUME,
        AVERAGE_INTEREST_RATE,
        GOLD_LOADED_AT
    ]
