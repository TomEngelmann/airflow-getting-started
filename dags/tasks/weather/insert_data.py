from airflow.hooks.postgres_hook import PostgresHook  # Import hinzugefügt
from airflow.exceptions import AirflowFailException
from datetime import datetime, timedelta

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