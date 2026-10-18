from Bronze.Mapper.Coingecko_Bronze_Mapper import map_to_bronze


# ==========================================
# COINGECKO BRONZE PROCESS
# ==========================================

def process(spark, raw_df):
    """Process CoinGecko BTC and ETH market data into Bronze."""

    bronze_df = map_to_bronze(raw_df)

    return bronze_df
