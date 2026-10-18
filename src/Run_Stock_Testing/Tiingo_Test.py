
from pathlib import Path

from pyspark.sql import SparkSession

from Bronze.Process.Tiingo_Bronze_Process import process as bronze_process


# ==========================================
# TIINGO BRONZE TEST
# ==========================================

def main():

    # STEP 1: Start Spark
    spark = (
        SparkSession.builder
        .appName("TiingoBronzeTest")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        # STEP 2: Find project root and raw folder
        project_root = Path(__file__).resolve().parents[2]

        raw_folder = (
            project_root
            / "data"
            / "raw"
        )

        matching_files = sorted(
            raw_folder.glob("TIINGO_KO_*.json")
        )

        print("\nSTEP 1: CHECK JSON FILE")

        if not matching_files:
            raise FileNotFoundError(
                f"No Tiingo KO JSON files found in {raw_folder}"
            )

        input_file = matching_files[-1]

        print("File path:", input_file)
        print("File exists:", input_file.exists())

        # STEP 3: Read raw JSON
        print("\nSTEP 2: READ RAW JSON")

        raw_df = (
            spark.read
            .option("multiLine", "true")
            .json(str(input_file))
        )

        print("\nRAW JSON SCHEMA")
        raw_df.printSchema()

        # STEP 4: Run Bronze Process
        print("\nSTEP 3: BRONZE PROCESS")

        bronze_df = bronze_process(spark, raw_df)

        print("\nBRONZE SCHEMA")
        bronze_df.printSchema()

        print("\nBRONZE RESULT")
        bronze_df.show(5, truncate=False)

        # STEP 5: Validate Bronze data
        print("\nSTEP 4: VALIDATE BRONZE DATA")

        row_count = bronze_df.count()

        print("BRONZE ROW COUNT:", row_count)

        if row_count == 0:
            raise RuntimeError(
                "Tiingo Bronze DataFrame contains no rows."
            )

        print("\nTIINGO BRONZE TEST COMPLETED SUCCESSFULLY")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
