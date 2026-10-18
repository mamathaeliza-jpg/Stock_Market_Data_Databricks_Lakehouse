
from Bronze.Mapper.Marketstack_Bronze_Mapper import map_to_bronze


# ==========================================
# MARKETSTACK BRONZE PROCESS
# ==========================================

def process(spark, raw_df):
    """Process Marketstack NFLX historical stock prices into Bronze."""

    bronze_df = map_to_bronze(raw_df)

    return bronze_df

