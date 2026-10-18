
from Bronze.Mapper.FRED_Bronze_Mapper import map_to_bronze


# ==========================================
# FRED BRONZE PROCESS
# ==========================================

def process(spark, raw_df):
    """Process FRED interest rate observations into Bronze."""

    bronze_df = map_to_bronze(raw_df)

    return bronze_df
