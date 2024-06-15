from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'simple_spark_dag',
    default_args=default_args,
    description='A simple DAG to run a Spark job reading from MinIO',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

spark_submit_task = SparkSubmitOperator(
    task_id='spark_submit_task',
    application='jobs/python/test2.py',  # Path to your Spark job
    name='spark_minio_job',
    conn_id='Spark_connection',
    conf={
        'spark.hadoop.fs.s3a.endpoint': 'http://host.docker.internal:9000',
        'spark.hadoop.fs.s3a.access.key': 'khaiVDT2024',
        'spark.hadoop.fs.s3a.secret.key': 'Khaichau_2k3',
        'spark.hadoop.fs.s3a.path.style.access': 'true'
    },
    dag=dag,
)

spark_submit_task
