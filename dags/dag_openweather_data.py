from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException
import requests

default_args = {
    'owner': 'Tom Engelmann',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

def call_api():
    url = "https://api.weatherapi.com/v1/current.json?key=71c1bebf81244262930110143232711&q=Faro&aqi=no"

    # Führe den GET-Request aus
    response = requests.get(url)

    # Überprüfe, ob die Anfrage erfolgreich war (Statuscode 200)
    if response.status_code == 200:
        # Die Antwort als JSON-Daten abrufen
        data = response.json()
        # Ausgabe der JSON-Daten als String
        print(data)
        return data  # Rückgabewert hier hinzugefügt
    else:
        # Ausgabe der Fehlermeldung, falls die Anfrage nicht erfolgreich war
        print(f"Fehler bei der Anfrage. Statuscode: {response.status_code}")
        raise AirflowFailException("failed api request")
        return None

with DAG(
    default_args = default_args,
    dag_id = 'dag_weather_api_v1',
    description = 'Dag for calling weather api',
    start_date = datetime(2023, 11, 27),
    schedule_interval = '@daily'
) as dag: 
    task1 = PythonOperator(
    task_id = 'get_data',
	python_callable = call_api
	)

    task1