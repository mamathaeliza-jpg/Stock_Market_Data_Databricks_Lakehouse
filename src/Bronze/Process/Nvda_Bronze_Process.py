
from Bronze.Mapper.Nvda_Bronze_Mapper import map_to_bronze


def process(spark, raw_df):
    """
    Process raw Twelve Data NVIDIA (NVDA)
    historical stock prices into the Bronze layer.
    """

    bronze_df = map_to_bronze(raw_df)

    return bronze_df
