from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
	'owner': 'Tom Engelmann',
	'retries': 5,
    'retry_delay': timedelta(minutes=5)
} 

def greet(name):
    print(f"Hello {name}")

with DAG(
		default_args = default_args,
		dag_id = 'dag_with_python_v2',
		description = 'First python operator',
		start_date = datetime(2023, 11, 27),
		schedule_interval = '@daily'
) as dag: 
    task1 = PythonOperator(
		task_id = 'greet',
		python_callable = greet,
		op_kwargs = {
			'name' : 'Tom'
		}
	)

    task1
