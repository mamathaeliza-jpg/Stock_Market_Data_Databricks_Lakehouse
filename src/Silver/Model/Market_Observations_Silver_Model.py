# ==========================================
# SILVER 5: MARKET OBSERVATIONS MODEL
# ==========================================

class Market_Observations_Silver_Model:

    # ==========================================
    # STEP 1: OUTPUT TABLE
    # ==========================================

    TABLE_NAME = "workspace.silver.fact_market_observations"

    # ==========================================
    # STEP 2: OBSERVATION IDENTIFIERS
    # ==========================================

    OBSERVATION_ID = "observation_id"
    ASSET_ID = "asset_id"
    SOURCE_ID = "source_id"

    # ==========================================
    # STEP 3: OBSERVATION ATTRIBUTES
    # ==========================================

    OBSERVATION_DATE = "observation_date"
    OBSERVATION_TYPE = "observation_type"

    METRIC_TYPE = "metric_type"
    METRIC_VALUE = "metric_value"

    # ==========================================
    # STEP 4: DATA LINEAGE
    # ==========================================

    SOURCE_NAME = "source_name"
    BRONZE_TABLE = "bronze_table"

    # ==========================================
    # STEP 5: AUDIT ATTRIBUTES
    # ==========================================

    SILVER_LOADED_AT = "silver_loaded_at"

    # ==========================================
    # STEP 6: MODEL COLUMNS
    # ==========================================

    COLUMNS = [
        OBSERVATION_ID,
        ASSET_ID,
        SOURCE_ID,
        OBSERVATION_DATE,
        OBSERVATION_TYPE,
        METRIC_TYPE,
        METRIC_VALUE,
        SOURCE_NAME,
        BRONZE_TABLE,
        SILVER_LOADED_AT
    ]

    # ==========================================
    # STEP 7: OBSERVATION CATEGORIES
    # ==========================================

    STOCK_PRICE = "STOCK_PRICE"
    STOCK_QUOTE = "STOCK_QUOTE"
    CRYPTO_MARKET = "CRYPTO_MARKET"
    INTEREST_RATE = "INTEREST_RATE"
