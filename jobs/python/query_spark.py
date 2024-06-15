from pyspark.sql import SparkSession
import sys

def main(file_url, access_key, secret_key):
    spark = SparkSession.builder \
        .appName("minio_csv_query") \
        .config("spark.hadoop.fs.s3a.access.key", access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .getOrCreate()
    
    # Load the CSV file
    df = spark.read.csv(file_url, header=True, inferSchema=True)

    # Perform some query, e.g., show the first 10 rows
    df.show(10)

    # Example of a simple transformation
    df_grouped = df.groupBy("some_column").count()
    df_grouped.show()

    spark.stop()

if __name__ == "__main__":
    file_url = sys.argv[1]
    access_key = sys.argv[2]
    secret_key = sys.argv[3]
    main(file_url, access_key, secret_key)