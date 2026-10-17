
from pyspark.sql import SparkSession

from Silver.Mapper.Alpha_Silver_Mapper import map_to_silver


def process(spark: SparkSession, bronze_df):

    print("BRONZE DATA")
    bronze_df.show(5, truncate=False)

    silver_df = map_to_silver(bronze_df)

    print("SILVER DATA")
    silver_df.show(5, truncate=False)

    return silver_df
