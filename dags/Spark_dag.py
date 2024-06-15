from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 12),
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    
}
MINIO_ENDPOINT = 'http://host.docker.internal:9000'
MINIO_ACCESS_KEY = 'khaiVDT2024'
MINIO_SECRET_KEY = 'Khaichau_2k3'
MINIO_BUCKET = 'data-lake'
MINIO_OBJECT = 'allstockprice.csv'
MINIO_URL = f's3a://{MINIO_BUCKET}/{MINIO_OBJECT}'

# Spark configurations
SPARK_APP_NAME = 'minio_csv_query'
SPARK_MASTER = 'spark://spark-master:7077'

dag = DAG(
    dag_id='Spark_dag',
    default_args=default_args,
    schedule_interval='@daily',
)
spark_task = SparkSubmitOperator(
    task_id='spark_query_csv',
    application='jobs/python/query_spark.py',
    #name=SPARK_APP_NAME,
    conn_id='Spark_connection',
    application_args=[MINIO_URL, MINIO_ACCESS_KEY, MINIO_SECRET_KEY],
    conf={
        'spark.master': SPARK_MASTER,
        'spark.hadoop.fs.s3a.endpoint': MINIO_ENDPOINT,
        'spark.hadoop.fs.s3a.access.key': MINIO_ACCESS_KEY,
        'spark.hadoop.fs.s3a.secret.key': MINIO_SECRET_KEY,
        'spark.hadoop.fs.s3a.path.style.access': 'true',
        'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
    },
    dag=dag,
)

spark_task