
from Bronze.Mapper.FMP_Bronze_Mapper import map_to_bronze


def process(spark, raw_df):
    """
    Process Financial Modeling Prep (FMP)
    Amazon (AMZN) historical stock prices
    into the Bronze layer.
    """

    # ==========================================
    # STEP 1: TRANSFORM RAW DATA INTO BRONZE
    # ==========================================

    bronze_df = map_to_bronze(raw_df)

    # ==========================================
    # STEP 2: RETURN BRONZE DATAFRAME
    # ==========================================

    return bronze_df
