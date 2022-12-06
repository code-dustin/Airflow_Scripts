from datetime import datetime,timedelta
import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

# Define the DAG
default_args = {
    'owner': 'dustin',
    'retries': 5, 
    'retry_delay': timedelta(minute=2),
    'start_date': datetime(2022, 12, 5),
    'schedule_interval': '@daily'
}

dag = DAG('api_to_postgres_example', default_args=default_args)

# Define the function that will be used by the PythonOperator
def get_data_from_api():
    response = requests.get('https://api.example.com/data')

    if response.status_code == 200:
        data = response.json()
        return data
    else:
        raise ValueError('Failed to retrieve data from API')

def insert_data_into_postgres(data):
    postgres_hook = PostgresHook(postgres_conn_id='my_postgres_conn')

    sql = """
        INSERT INTO my_table (field1, field2, field3)
        VALUES (%s, %s, %s)
    """

    # Iterate over the data and execute the SQL statement for each item
    for item in data:
        postgres_hook.run(sql, parameters=(item['field1'], item['field2'], item['field3']))

get_data_task = PythonOperator(
    task_id='get_data_from_api',
    python_callable=get_data_from_api,
    dag=dag
)

insert_data_task = PythonOperator(
    task_id='insert_data_into_postgres',
    python_callable=insert_data_into_postgres,
    dag=dag,
    provide_context=True
)

get_data_task >> insert_data_task
