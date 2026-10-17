# Databricks notebook source

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

data = [
    ("AAPL", 250.50),
    ("MSFT", 510.20),
    ("GOOGL", 310.75),
]

df = spark.createDataFrame(
    data,
    ["symbol", "close_price"]
)

df.show()
