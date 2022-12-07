from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.snowflake_operator import SnowflakeOperator
from datetime import datetime, timedelta
import requests
import csv

default_args = {
    'owner': 'dustin',
    'start_date': datetime(2022, 12, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('api_to_snowflake', default_args=default_args, schedule_interval='@daily')

def get_api_data(**kwargs):
    url = 'https://api.example.com/data'

    headers = {
        'Authorization': 'Bearer MY_SECRET_TOKEN',
        'Content-Type': 'application/json',
    }

    params = {
        'start_date': '2022-12-01',
        'end_date': '2022-12-31',
    }
    response = requests.get(url, headers=headers, params=params)
    data = response.json()

    if 'key' not in data:
        raise ValueError('Data does not contain key')

    return data

def transform_data(data, **kwargs):
    csv_file = open('data.csv', 'w', newline='')
    writer = csv.writer(csv_file)

    writer.writerow(['key', 'first_name', 'last_name'])

    for item in data:
        writer.writerow([item['key'], item['first_name'], item['last_name']])

    csv_file.close()

    return 'data.csv'

get_api_data_task = PythonOperator(
    task_id='get_api_data',
    python_callable=get_api_data,
    dag=dag,
)

transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    op_kwargs={'data': '{{ task_instance.xcom_pull(task_ids="get_api_data") }}'},
    dag=dag,
)

#Assumes table has already been created in Snowflake, if not, add in create if not exists within sql.
load_data_task = SnowflakeOperator(
    task_id='load_data',
    snowflake_conn_id='snowflake_default',
    sql='LOAD DATA INFILE @data_file INTO TABLE my_table',
    parameters={'data_file': '{{ task_instance.xcom_pull(task_ids="transform_data") }}'},
    dag=dag,
)

get_api_data_task >> transform_data_task >> load_data_task
