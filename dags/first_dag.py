from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
	'owner': 'Tom Engelmann',
	'retries': 5,
	'retry_delay': timedelta(minutes=2)
}

with DAG(
	dag_id = 'first_dag_v1',
	default_args = default_args,
	description = 'This is the first dag',
	start_date = datetime(2023, 12, 1, 2), # starting at 1.12.23 every day at 2am
	schedule_interval = '@daily'
) as dag:
	task1 = BashOperator(
		task_id = 'first_task',
		bash_command = "echo hello world, this is the first task!"
	)

	task2 = BashOperator(
		task_id = 'second_task',
		bash_command = "echo I'm the second task"
	)

	task3 = BashOperator(
		task_id = 'third_task',
		bash_command = "echo I'm the thirds task, running at the same time as 2"
	)

	# define the DAG 1 -> 2,3

	# Task dependency method 1:
	# task1.set_downstream(task2)
	# task1.set_downstream(task3)

	# Task dependency method 2: 
	task1 >> task2
	task1 >> task3

	# Task dependency method 3:
	task1 >> [task2, task3]