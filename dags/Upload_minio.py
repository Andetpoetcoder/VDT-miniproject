from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

# Connect to the CrawlData.py (python file with all function to crawl data)
import sys
sys.path.append('/opt/airflow/code')
import CrawlData
# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 12),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    
}

# Initialize the DAG
dag = DAG(
    dag_id='minio_upload_dag',
    default_args=default_args,
    schedule_interval='@daily',
)

# Define the upload function
def upload_to_minio(file_path, bucket_name, object_name):
    hook = S3Hook(aws_conn_id='minio_s3_connection')
    hook.load_file(filename=file_path, key=object_name, bucket_name=bucket_name, replace=True)
def crawl_data():
    CrawlData.get_full_list_stock_price()
    CrawlData.get_vnindex()
# Create the task
files_to_upload = [
    {
        'file_path': "/opt/airflow/data/allstockprice.csv",
        'bucket_name': 'data-lake',
        'object_name': 'allstockprice.csv',
    },
    {
        'file_path': "/opt/airflow/data/vnindex.csv",
        'bucket_name': 'data-lake',
        'object_name': 'vnindex.csv',
    },
]
upload_tasks = []
for index, file_info in enumerate(files_to_upload):
    task_id = f'upload_file_to_minio_{index}'
    upload_task = PythonOperator(
        task_id=task_id,
        python_callable=upload_to_minio,
        op_kwargs=file_info,
        dag=dag,
    )
    upload_tasks.append(upload_task)

crawl_data_task = PythonOperator(
    task_id='crawl_data_task',
    python_callable=crawl_data,
    dag=dag,
)

query_job = SparkSubmitOperator(
    task_id="query_job",
    conn_id="spark-conn",
    application="jobs/python/Query_Spark.py",
    conf={
        "spark.driver.memory": "1g",  
        "spark.executor.memory": "1g", 
        "spark.executor.instances": "1" 
    },
    dag=dag
)

# Set the task in the DAG
[upload_tasks[0],upload_tasks[1]]>>query_job
