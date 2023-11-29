import requests
from airflow.exceptions import AirflowFailException

def call_api():
    url = "https://api.weatherapi.com/v1/current.json?key=71c1bebf81244262930110143232711&q=Faro&aqi=no"

    # Führe den GET-Request aus
    response = requests.get(url)

    # Überprüfe, ob die Anfrage erfolgreich war (Statuscode 200)
    if response.status_code == 200:
        # Die Antwort als JSON-Daten abrufen
        data = response.json()
        # Ausgabe der JSON-Daten als String
        return data  # Rückgabewert hier hinzugefügt
    else:
        # Ausgabe der Fehlermeldung, falls die Anfrage nicht erfolgreich war
        print(f"Fehler bei der Anfrage. Statuscode: {response.status_code}")
        raise AirflowFailException("failed api request")
        return None