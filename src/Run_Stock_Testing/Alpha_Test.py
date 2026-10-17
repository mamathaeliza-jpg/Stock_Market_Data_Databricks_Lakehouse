
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import MapType, StringType

# BRONZE
from Bronze.Mapper.Alpha_Mapper import map_to_bronze

# SILVER
from Silver.Process.Alpha_Silver_Process import process as silver_process

# GOLD
from Gold.Process.Alpha_Gold_Process import process as gold_process


def main():

    # STEP 1: Start Spark
    spark = (
        SparkSession.builder
        .appName("StockMarketBronzeSilverGoldTest")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        # STEP 2: Locate project root
        project_root = Path(__file__).resolve().parents[2]

        input_file = (
            project_root
            / "data"
            / "raw"
            / "AAPL_20261017T154515Z.json"
        )

        print("\n========== STEP 1: CHECK JSON FILE ==========")
        print("File path:", input_file)
        print("File exists:", input_file.exists())

        if not input_file.exists():
            raise FileNotFoundError(
                f"JSON file not found: {input_file}"
            )

        # STEP 3: Read raw JSON
        print("\n========== STEP 2: READ RAW JSON ==========")

        raw_df = (
            spark.read
            .option("multiLine", "true")
            .json(str(input_file))
        )

        raw_df.printSchema()

        # STEP 4: Prepare daily prices as a Spark MapType
        print("\n========== STEP 3: PREPARE BRONZE INPUT ==========")

        daily_prices = F.col("payload").getField(
            "Time Series (Daily)"
        )

        raw_df = raw_df.withColumn(
            "payload",
            F.struct(
                F.from_json(
                    F.to_json(daily_prices),
                    MapType(
                        StringType(),
                        MapType(StringType(), StringType())
                    )
                ).alias("Time Series (Daily)")
            )
        )

        # STEP 5: Bronze Mapping
        print("\n========== STEP 4: BRONZE PROCESS ==========")

        bronze_df = map_to_bronze(raw_df)

        bronze_df.show(5, truncate=False)

        bronze_count = bronze_df.count()
        print("BRONZE ROW COUNT:", bronze_count)

        # STEP 6: Silver Process
        print("\n========== STEP 5: SILVER PROCESS ==========")

        silver_df = silver_process(spark, bronze_df)

        silver_df.show(5, truncate=False)

        silver_count = silver_df.count()
        print("SILVER ROW COUNT:", silver_count)

        # STEP 7: Gold Process
        print("\n========== STEP 6: GOLD PROCESS ==========")

        gold_df = gold_process(spark, silver_df)

        gold_df.orderBy("trade_date").show(
            10,
            truncate=False
        )

        gold_count = gold_df.count()
        print("GOLD ROW COUNT:", gold_count)

        # STEP 8: Final Results
        print("\n========== PIPELINE SUMMARY ==========")

        print("Bronze Rows:", bronze_count)
        print("Silver Rows:", silver_count)
        print("Gold Rows:", gold_count)

        print(
            "\nBRONZE -> SILVER -> GOLD "
            "TEST COMPLETED SUCCESSFULLY"
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
