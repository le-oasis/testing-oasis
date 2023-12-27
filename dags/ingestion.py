from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from sql import query_trip_data
from clickhouse_driver import Client
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'clickhouse_to_sqlite_dag', default_args=default_args, schedule_interval=None)

def execute_query():
    host = os.getenv('cl_host')
    user = os.getenv('cl_user')
    password = os.getenv('cl_password')
    port = os.getenv('cl_port')
    database = os.getenv('cl_database')
    
    client = Client(host=host, user=user, password=password, port=port, database=database, secure=True)
    result = client.execute(query_trip_data)  # Execute the SQL query
    return result

def write_to_sqlite(results):
    # Function body here
    pass

execute_query_task = PythonOperator(
    task_id='execute_query',
    python_callable=execute_query,
    dag=dag,
    provide_context=True,
)

write_to_sqlite_task = PythonOperator(
    task_id='write_to_sqlite',
    python_callable=write_to_sqlite,
    op_args=['{{ ti.xcom_pull(task_ids="execute_query") }}'],
    dag=dag,
    provide_context=True,
)

execute_query_task >> write_to_sqlite_task