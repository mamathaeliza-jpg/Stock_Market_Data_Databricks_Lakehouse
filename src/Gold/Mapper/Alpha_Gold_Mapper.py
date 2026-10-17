
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from Gold.Model import Alpha_Gold_Model as M


def map_to_gold(silver_df):

    # Group calculations by stock symbol
    window = Window.partitionBy("symbol").orderBy("trade_date")

    window_7 = window.rowsBetween(-6, 0)
    window_30 = window.rowsBetween(-29, 0)

    return (
        silver_df
        .withColumn(
            M.PREVIOUS_CLOSE,
            F.lag("close_price").over(window)
        )
        .withColumn(
            M.DAILY_RETURN_PCT,
            F.when(
                F.col(M.PREVIOUS_CLOSE) != 0,
                F.round(
                    (
                        (F.col("close_price") - F.col(M.PREVIOUS_CLOSE))
                        / F.col(M.PREVIOUS_CLOSE)
                    ) * 100,
                    2
                )
            )
        )
        .withColumn(
            M.MOVING_AVG_7,
            F.round(
                F.avg("close_price").over(window_7),
                2
            )
        )
        .withColumn(
            M.MOVING_AVG_30,
            F.round(
                F.avg("close_price").over(window_30),
                2
            )
        )
        .select(
            F.col("symbol").alias(M.SYMBOL),
            F.col("trade_date").alias(M.TRADE_DATE),
            F.col("close_price").alias(M.CLOSE_PRICE),
            F.col(M.PREVIOUS_CLOSE),
            F.col(M.DAILY_RETURN_PCT),
            F.col(M.MOVING_AVG_7),
            F.col(M.MOVING_AVG_30),
            F.col("volume").alias(M.VOLUME)
        )
    )
