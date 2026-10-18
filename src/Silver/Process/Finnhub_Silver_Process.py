from Silver.Mapper.Finnhub_Silver_Mapper import map_to_silver


def process(spark, bronze_df):
    """
    Process Finnhub Microsoft (MSFT)
    Bronze stock quotes into the Silver layer.
    """

    silver_df = map_to_silver(bronze_df)

    return silver_df
