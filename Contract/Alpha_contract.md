
# ==========================================
# BRONZE: ALPHA VANTAGE STOCK PRICES CONTRACT
# ==========================================


class Alpha_Contract:

    # ==========================================
    # STEP 1: TARGET TABLE
    # ==========================================

    TABLE_NAME = "bronze_stock_prices"

    # ==========================================
    # STEP 2: STOCK IDENTIFICATION
    # ==========================================

    SYMBOL = "symbol"
    TRADE_DATE = "trade_date"

    # ==========================================
    # STEP 3: STOCK PRICE COLUMNS
    # ==========================================

    OPEN = "open"
    HIGH = "high"
    LOW = "low"
    CLOSE = "close"
    VOLUME = "volume"

    # ==========================================
    # STEP 4: SOURCE AND AUDIT COLUMNS
    # ==========================================

    SOURCE = "source"
    BATCH_ID = "batch_id"
    INGESTED_AT = "ingested_at"
    BRONZE_LOADED_AT = "bronze_loaded_at"

    # ==========================================
    # STEP 5: REQUIRED COLUMNS
    # ==========================================

    REQUIRED_COLUMNS = [
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
        BRONZE_LOADED_AT
    ]

    # ==========================================
    # STEP 6: BUSINESS KEY
    # ==========================================

    BUSINESS_KEY = [
        SYMBOL,
        TRADE_DATE
    ]

    # ==========================================
    # STEP 7: EXPECTED DATA TYPES
    # ==========================================

    COLUMN_TYPES = {
        SYMBOL: "string",
        TRADE_DATE: "date",
        OPEN: "double",
        HIGH: "double",
        LOW: "double",
        CLOSE: "double",
        VOLUME: "long",
        SOURCE: "string",
        BATCH_ID: "string",
        INGESTED_AT: "timestamp",
        BRONZE_LOADED_AT: "timestamp"
    }

    # ==========================================
    # STEP 8: VALIDATE DATAFRAME COLUMNS
    # ==========================================

    @staticmethod
    def validate_columns(df):

        missing_columns = [
            column
            for column in Alpha_Contract.REQUIRED_COLUMNS
            if column not in df.columns
        ]

        if missing_columns:
            raise ValueError(
                f"Missing Alpha Bronze columns: {missing_columns}"
            )

        return True
