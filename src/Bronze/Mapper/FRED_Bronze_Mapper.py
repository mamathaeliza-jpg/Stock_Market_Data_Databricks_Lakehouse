
from pyspark.sql import functions as F

from Bronze.Model import FRED_Bronze_Model as M


# ==========================================
# FRED BRONZE MAPPER
# ==========================================

def map_to_bronze(raw_df):

    # STEP 1: Explode FRED observations
    observations = F.explode(
        F.col("payload.observations")
    ).alias("observation")

    # STEP 2: Map JSON fields to Bronze columns
    bronze_df = (
        raw_df
        .select(
            F.col("series_id"),
            F.col("source"),
            F.col("batch_id"),
            F.col("ingested_at"),
            observations
        )
        .select(
            F.col("series_id").alias(M.SERIES_ID),
            F.col("observation.date").alias(
                M.OBSERVATION_DATE
            ),
            F.col("observation.value").cast("string").alias(
                M.VALUE
            ),
            F.col("source").alias(M.SOURCE),
            F.col("batch_id").alias(M.BATCH_ID),
            F.col("ingested_at").alias(M.INGESTED_AT)
        )
    )

    # STEP 3: Add Bronze load timestamp
    bronze_df = bronze_df.withColumn(
        M.BRONZE_LOADED_AT,
        F.current_timestamp()
    )

    # STEP 4: Return Bronze columns in model order
    return bronze_df.select(*M.BRONZE_COLUMNS)
