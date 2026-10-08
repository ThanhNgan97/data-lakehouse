"""Minimal Spark application used only while building the Airflow image."""

from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .master("local[1]")
    .appName("resolve-lakehouse-jars")
    .getOrCreate()
)
spark.stop()
