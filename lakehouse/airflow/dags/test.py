from airflow import DAG
from airflow.hooks.clickhouse_hook import ClickHouseHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.sqlite_operator import SqliteOperator
from airflow.utils.dates import days_ago
from datetime import datetime 

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 10, 18)    
}

def _data_from_clickhouse():
    ch_hook = ClickHouseHook(clickhouse_conn_id='ClickHouse_rnd_conn')   
    query = """
        SELECT 
            toMonth(date) as month,
            toYear(date) as year,
            avgIf(duration, toDayOfWeek(date) = 6) as avg_duration_saturday,
            avgIf(fare_amount, toDayOfWeek(date) = 6) as avg_fare_saturday,
            countIf(id, toDayOfWeek(date) = 6) as avg_trips_saturday,
            avgIf(duration, toDayOfWeek(date) = 7) as avg_duration_sunday,
            avgIf(fare_amount, toDayOfWeek(date) = 7) as avg_fare_sunday,
            countIf(id, toDayOfWeek(date) = 7) as avg_trips_sunday
        FROM tripdata
        WHERE date BETWEEN '2014-01-01' AND '2016-12-31'
        GROUP BY year, month
        ORDER BY year, month
    """
    records = ch_hook.get_records(query)
    return records

with DAG(dag_id='data_to_clickhouse',          
         default_args=default_args) as dag:  
        
    get_data_from_clickhouse = PythonOperator(
        task_id='get_data_from_clickhouse',
        python_callable=_data_from_clickhouse,
    )
    
    insert_into_sqlite = SqliteOperator(
        task_id='insert_into_sqlite',
        sqlite_conn_id='sqlite_default',
        sql="""
        INSERT INTO my_table (month, year, avg_duration_saturday, avg_fare_saturday, avg_trips_saturday, avg_duration_sunday, avg_fare_sunday, avg_trips_sunday)
        VALUES (:month, :year, :avg_duration_saturday, :avg_fare_saturday, :avg_trips_saturday, :avg_duration_sunday, :avg_fare_sunday, :avg_trips_sunday)
        """,
        parameters=get_data_from_clickhouse.output,
    )
    
    get_data_from_clickhouse >> insert_into_sqlite