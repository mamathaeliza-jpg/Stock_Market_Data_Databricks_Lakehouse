
from pyspark.sql import SparkSession

from Gold.Mapper.Alpha_Gold_Mapper import map_to_gold


def process(spark: SparkSession, silver_df):

    print("GOLD ANALYTICS PROCESS")

    gold_df = map_to_gold(silver_df)

    gold_df.orderBy("trade_date").show(10, truncate=False)

    return gold_df
