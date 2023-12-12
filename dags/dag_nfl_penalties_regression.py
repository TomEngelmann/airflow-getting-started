from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'Tom Engelmann',
    'depends_on_past': False,
    'start_date': datetime(2023, 12, 11),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
dag = DAG(
    'linear_regression_dag',
    default_args=default_args,
    description='Linear Regression DAG',
    schedule_interval='@daily',
)

def run_linear_regression_model():
    exec(open('/opt/airflow/dags/tasks/nfl/linear_regression.py').read())

def run_prediction():
    exec(open('/opt/airflow/dags/tasks/nfl/prediction_model.py').read())

def run_endpoint():
    exec(open('/opt/airflow/dags/tasks/nfl/rest_route.py').read())

run_model_task = PythonOperator(
    task_id='run_linear_regression_model',
    python_callable=run_linear_regression_model,
    dag=dag,
)

predict_task = PythonOperator(
    task_id='predict_model',
    python_callable=run_prediction,
    dag=dag,
)

endpoint = PythonOperator(
    task_id='run_endpoint',
    python_callable=run_endpoint,
    dag=dag,
)

run_model_task >> predict_task >> endpoint