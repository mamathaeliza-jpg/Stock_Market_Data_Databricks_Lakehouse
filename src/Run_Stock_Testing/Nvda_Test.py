
from Silver.Mapper.Nvda_Silver_Mapper import map_to_silver


def process(spark, bronze_df):
    """
    Process Twelve Data NVIDIA (NVDA)
    Bronze stock prices into the Silver layer.
    """

    silver_df = map_to_silver(bronze_df)

    return silver_df
