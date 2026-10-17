
from pyspark.sql import functions as F

from Silver.Model import Alpha_Silver_Model as M


def map_to_silver(bronze_df):

    return (
        bronze_df
        .select(
            F.upper(F.trim("symbol")).alias(M.SYMBOL),
            F.to_date("trade_date").alias(M.TRADE_DATE),
            F.col("open").cast("decimal(18,4)").alias(M.OPEN_PRICE),
            F.col("high").cast("decimal(18,4)").alias(M.HIGH_PRICE),
            F.col("low").cast("decimal(18,4)").alias(M.LOW_PRICE),
            F.col("close").cast("decimal(18,4)").alias(M.CLOSE_PRICE),
            F.col("volume").cast("long").alias(M.VOLUME),
            F.to_timestamp("ingested_at").alias(M.INGESTED_AT),
        )
        .withColumn(
            M.STOCK_HASH_ID,
            F.sha2(
                F.concat_ws(
                    "|",
                    F.col(M.SYMBOL),
                    F.col(M.TRADE_DATE).cast("string")
                ),
                256
            )
        )
    )
