
from Bronze.Mapper.Tiingo_Bronze_Mapper import map_to_bronze


# ==========================================
# TIINGO BRONZE PROCESS
# ==========================================

def process(spark, raw_df):
    """Process Tiingo KO historical stock prices into Bronze."""

    # STEP 1: Transform raw JSON into Bronze DataFrame
    bronze_df = map_to_bronze(raw_df)

    # STEP 2: Return Bronze DataFrame
    return bronze_df
