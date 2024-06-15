from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

# Local file path
LOCAL_FILE_PATH = '/Data/allstockprice.csv'  # Update this path to your local CSV file

# Spark configurations
SPARK_APP_NAME = 'local_csv_query'
SPARK_MASTER = 'spark://spark-master:7077'
SPARK_APPLICATION_PATH = '/jobs/python/test.py'  # Update this path to your Spark application

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 14),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'local_to_spark',
    default_args=default_args,
    description='Query local CSV using Spark',
    schedule_interval=timedelta(days=1),
)

spark_task = SparkSubmitOperator(
    task_id='spark_query_csv',
    application=SPARK_APPLICATION_PATH,
    name=SPARK_APP_NAME,
    conn_id='Spark_connection',
    application_args=[LOCAL_FILE_PATH],
    conf={
        'spark.master': SPARK_MASTER,
    },
    dag=dag,
)

spark_task
