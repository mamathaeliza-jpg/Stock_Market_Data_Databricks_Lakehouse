from Bronze.Mapper.Massive_Bronze_Mapper import map_to_bronze


# ==========================================
# MASSIVE BRONZE PROCESS
# ==========================================

def process(spark, raw_df):
    """Process Massive TSLA historical stock prices into Bronze."""

    bronze_df = map_to_bronze(raw_df)

    return bronze_df
