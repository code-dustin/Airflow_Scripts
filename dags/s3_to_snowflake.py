import json
import csv

from airflow import DAG
from airflow.operators.s3_download_operator import S3DownloadOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.snowflake_operator import SnowflakeOperator

default_args = {
    'start_date': datetime(2022, 12, 8),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('s3_to_snowflake_dag', default_args=default_args, schedule_interval='@daily')

snowflake_conn_id = 'snowflake_default'

def download_json_file(**kwargs):
    s3_client = boto3.client('s3')
    s3_client.download_file('my_bucket', 'my_file.json', '/tmp/my_file.json')

def transform_json_to_csv(**kwargs):
    with open('/tmp/my_file.json') as json_file, open('/tmp/my_file.csv', 'w') as csv_file:
        data = json.load(json_file)
        writer = csv.DictWriter(csv_file, fieldnames=['key_field'])
        writer.writeheader()
        for item in data:
            writer.writerow({'key_field': item['key_field']})

def load_data_into_snowflake(**kwargs):
    snowflake_conn = kwargs['task_instance'].xcom_pull(task_ids='download_json_file')
    cursor = snowflake_conn.cursor()
    cursor.execute(f"COPY INTO my_table FROM '@%/tmp/my_file.csv'")
    cursor.commit()

def cluster_data(**kwargs):
    snowflake_conn = kwargs['task_instance'].xcom_pull(task_ids='download_json_file')
    cursor = snowflake_conn.cursor()
    cursor.execute(f"CLUSTER my_table BY key_field")

download_json_file = PythonOperator(
    task_id='download_json_file',
    python_callable=download_json_file,
    provide_context=True,
    dag=dag,
)

transform_json_to_csv = PythonOperator(
    task_id='transform_json_to_csv',
    python_callable=transform_json_to_csv,
    provide_context=True,
    dag=dag,
)

load_data_into_snowflake = SnowflakeOperator(
    task_id='load_data_into_snowflake',
    snowflake_conn_id=snowflake_conn_id,
    sql=load_data_into_snowflake,
    params=None,
    autocommit=True,
    dag=dag,
)

cluster_data = PythonOperator(
    task_id='cluster_data',
    python_callable=cluster_data,
    provide_context=True,
    dag=dag,
)

# Set up the dependencies between the tasks
download_json_file >> transform_json_to_csv >> load_data_into_snowflake >> cluster_data