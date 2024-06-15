from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName('Spark MinIO Example') \
    .getOrCreate()

# Replace with your MinIO bucket and file path
minio_url = 's3a://data-lake/vnindex.csv'

df = spark.read.csv(minio_url, header=True, inferSchema=True)
df.show(10)


spark.stop()
