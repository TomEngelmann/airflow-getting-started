from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

# Import tasks from task directory
from tasks.weather.api import call_api
from tasks.weather.insert_data import insert_data_to_postgres
from tasks.weather.get_data import get_data_from_postgres

default_args = {
    'owner': 'Tom Engelmann',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    default_args = default_args,
    dag_id = 'dag_weather_api_v2',
    description = 'Dag for calling weather api',
    start_date = datetime(2023, 11, 27),
    schedule_interval = '@daily'
) as dag: 
    task1 = PythonOperator(
    task_id = 'get_data',
	python_callable = call_api
	)

    task2 = PostgresOperator(
		task_id = 'create_postgres_table',
		postgres_conn_id = 'postgres_localhost',
		sql = """
			CREATE TABLE IF NOT EXISTS weather_data (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255),
            country VARCHAR(255),
            time_stamp TIMESTAMP,
            local_time TIMESTAMP,
            temp_c FLOAT,
            feelslike_c FLOAT,
            wind_kph FLOAT
        );
		"""
	)

    task3 = PythonOperator(
        task_id='insert_data_to_postgres',
        python_callable=insert_data_to_postgres,
        provide_context=True,
    )

    task4 = PythonOperator(
        task_id='get_data_from_postgres',
        python_callable=get_data_from_postgres,
    )

    task1 >> task2 >> task3 >> task4