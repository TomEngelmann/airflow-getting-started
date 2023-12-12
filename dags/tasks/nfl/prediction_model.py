import joblib
import pandas as pd

# Lade das gespeicherte Modell
loaded_model = joblib.load('/opt/airflow/dags/tasks/nfl/linear_regression_model.pkl')

# Annahme: Du hast neue Daten, auf die du Vorhersagen machen möchtest
new_data = pd.DataFrame({
    'Yards': [56, 100, 23],
    'Week': [1, 2, 3],
    'Year': [2023, 2023, 2023]
})

# Mache Vorhersagen auf den neuen Daten
predictions = loaded_model.predict(new_data)

# Zeige die Vorhersagen an
print("Vorhersagen:")
print(predictions)
