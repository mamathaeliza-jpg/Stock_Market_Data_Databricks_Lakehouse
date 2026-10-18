
from Bronze.Mapper.EODHD_Bronze_Mapper import map_to_bronze


# ==========================================
# EODHD BRONZE PROCESS
# ==========================================

def process(spark, raw_df):
    """Process EODHD JPM historical stock prices into Bronze."""

    bronze_df = map_to_bronze(raw_df)

    return bronze_df
