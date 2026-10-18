
# ==========================================
# SILVER 6: MARKET INTELLIGENCE MODEL
# ==========================================

class Market_Intelligence_Silver_Model:

    # ==========================================
    # STEP 1: OUTPUT TABLE
    # ==========================================

    TABLE_NAME = "workspace.silver.fact_market_intelligence"

    # ==========================================
    # STEP 2: IDENTIFIERS
    # ==========================================

    INTELLIGENCE_ID = "intelligence_id"
    ASSET_ID = "asset_id"
    SOURCE_ID = "source_id"

    # ==========================================
    # STEP 3: MARKET INFORMATION
    # ==========================================

    OBSERVATION_DATE = "observation_date"
    METRIC_TYPE = "metric_type"
    METRIC_VALUE = "metric_value"

    # ==========================================
    # STEP 4: MARKET INTELLIGENCE
    # ==========================================

    PREVIOUS_CLOSE = "previous_close"
    PRICE_CHANGE = "price_change"
    PERCENTAGE_CHANGE = "percentage_change"
    DAILY_RANGE = "daily_range"

    # ==========================================
    # STEP 5: DATA LINEAGE
    # ==========================================

    SOURCE_NAME = "source_name"

    # ==========================================
    # STEP 6: AUDIT TIMESTAMP
    # ==========================================

    SILVER_LOADED_AT = "silver_loaded_at"

    # ==========================================
    # STEP 7: FINAL MODEL COLUMNS
    # ==========================================

    COLUMNS = [
        INTELLIGENCE_ID,
        ASSET_ID,
        SOURCE_ID,
        OBSERVATION_DATE,
        METRIC_TYPE,
        METRIC_VALUE,
        PREVIOUS_CLOSE,
        PRICE_CHANGE,
        PERCENTAGE_CHANGE,
        DAILY_RANGE,
        SOURCE_NAME,
        SILVER_LOADED_AT
    ]
