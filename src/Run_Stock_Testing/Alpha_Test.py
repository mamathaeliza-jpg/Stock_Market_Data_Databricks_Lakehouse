
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import MapType, StringType

from Bronze.Mapper.Alpha_Mapper import map_to_bronze
from Silver.Process.Alpha_Silver_Process import process as silver_process

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
        # STEP 2: Find the project folder
        project_root = Path(__file__).resolve().parents[2]

        input_file = (
            project_root
            / "data"
            / "raw"
            / "AAPL_20261017T154515Z.json"
        )

        print("\nSTEP 1: CHECK JSON FILE")
        print("File path:", input_file)
        print("File exists:", input_file.exists())

        if not input_file.exists():
            raise FileNotFoundError(
                f"JSON file not found: {input_file}"
            )

        # STEP 3: Read the downloaded JSON
        print("\nSTEP 2: READ RAW JSON")

        raw_df = (
            spark.read
            .option("multiLine", "true")
            .json(str(input_file))
        )

        print("\nRAW JSON SCHEMA")
        raw_df.printSchema()

        # STEP 4: Prepare the daily prices as a map
        # This is needed by our existing Bronze mapper.
        print("\nSTEP 3: PREPARE BRONZE INPUT")

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

        # STEP 5: Run Bronze Mapper
        print("\nSTEP 4: BRONZE MAPPING")

        bronze_df = map_to_bronze(raw_df)

        print("\nBRONZE RESULT")
        bronze_df.show(5, truncate=False)

        print("BRONZE ROW COUNT:", bronze_df.count())

        # STEP 6: Run Silver Process
        print("\nSTEP 5: SILVER PROCESS")

        silver_df = silver_process(spark, bronze_df)

        print("\nSILVER RESULT")
        silver_df.show(5, truncate=False)

        print("SILVER ROW COUNT:", silver_df.count())

        print("\nBRONZE TO SILVER TEST COMPLETED SUCCESSFULLY")

        # STEP 7: Run Gold Process
        print("\nSTEP 6: GOLD PROCESS")

        gold_df = gold_process(spark, silver_df)

        print("\nGOLD RESULT")
        gold_df.show(5, truncate=False)

        print("GOLD ROW COUNT:", gold_df.count())

        print("\nBRONZE → SILVER → GOLD TEST COMPLETED SUCCESSFULLY")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
