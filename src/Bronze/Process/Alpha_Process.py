import json

from Bronze.Mapper.Alpha_Mapper import map_to_bronze


def process(spark, input_path, bronze_table):

    # Suitable for our small API response POC
    with open(input_path, "r", encoding="utf-8") as file:
        raw_json = json.load(file)

    bronze_df = map_to_bronze(spark, raw_json)

    bronze_df.write.format("delta").mode("append").saveAsTable(
        bronze_table
    )

    return bronze_df