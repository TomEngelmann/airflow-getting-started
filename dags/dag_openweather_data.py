from datetime import datetime, timedelta
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook  # Import hinzugefügt

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

def insert_data_to_postgres(**kwargs):
    postgres_conn_id = 'postgres_localhost'

    # Extrahiere die Daten aus dem übergebenen Kontext (kwargs)
    data = kwargs['ti'].xcom_pull(task_ids='get_data')

    # Überprüfe, ob die erwarteten Schlüssel vorhanden sind
    if 'location' in data and 'current' in data:
        # Extrahiere die gewünschten Werte für die Tabelle
        insert_data = {
            'name': data['location']['name'],
            'country': data['location']['country'],
            'time_stamp': datetime.now(),
            'local_time': data['location']['localtime'],
            'temp_c': data['current']['temp_c'],
            'feelslike_c': data['current']['feelslike_c'],
            'wind_kph': data['current']['wind_kph'],
        }

        # SQL-Statement zum Einfügen der Daten
        insert_sql = """
        INSERT INTO weather_data (name, country, time_stamp, local_time, temp_c, feelslike_c, wind_kph)
        VALUES (%(name)s, %(country)s, %(time_stamp)s, %(local_time)s, %(temp_c)s, %(feelslike_c)s, %(wind_kph)s);
        """

        # Verbindung zu PostgreSQL herstellen
        pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)

        # Daten einfügen
        pg_hook.run(sql=insert_sql, parameters=insert_data)
    else:
        print("Ungültige Datenstruktur. Erforderliche Schlüssel fehlen.")

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

    task1 >> task2 >> task3