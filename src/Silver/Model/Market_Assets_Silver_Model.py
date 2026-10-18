# ==========================================
# SILVER 4: MARKET ASSETS MODEL
# ==========================================
# PURPOSE:
# Define standardized asset and source fields
# for the Silver 3NF reference layer.
# ==========================================


class Market_Assets_Silver_Model:

    # ==========================================
    # STEP 1: OUTPUT TABLE
    # ==========================================

    TABLE_NAME = "workspace.silver.dim_market_assets"

    # ==========================================
    # STEP 2: ASSET ATTRIBUTES
    # ==========================================

    ASSET_ID = "asset_id"
    SYMBOL = "symbol"
    ASSET_NAME = "asset_name"
    ASSET_TYPE = "asset_type"

    # ==========================================
    # STEP 3: SOURCE ATTRIBUTES
    # ==========================================

    SOURCE_ID = "source_id"
    SOURCE_NAME = "source_name"

    # ==========================================
    # STEP 4: AUDIT ATTRIBUTES
    # ==========================================

    SILVER_LOADED_AT = "silver_loaded_at"

    # ==========================================
    # STEP 5: OUTPUT COLUMN MAPPING
    # ==========================================

    COLUMNS = [
        ASSET_ID,
        SYMBOL,
        ASSET_NAME,
        ASSET_TYPE,
        SOURCE_ID,
        SOURCE_NAME,
        SILVER_LOADED_AT
    ]

