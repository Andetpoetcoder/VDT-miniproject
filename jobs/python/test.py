from pyspark.sql import SparkSession
import sys


# Initialize Spark session
spark = SparkSession.builder \
    .appName("Local CSV Reader") \
    .getOrCreate()

# Read the CSV file
df = spark.read.csv("/Data/allstockprice.csv", header=True, inferSchema=True)

# Perform a simple transformation, e.g., show the first 10 rows
df.show(10)

# Example of a simple transformation

spark.stop()

