# linear_regression_model.py

import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
import joblib

# Lade deine Daten (ersetze 'deine_daten.csv' durch den tatsächlichen Dateipfad)
data = pd.read_csv('/opt/airflow/dags/tasks/archive/penalties.csv')

# Filter Daten, sodass Set nur Personal Fouls enthält
filtered_data = data[data['Name'] == 'Personal Foul']

# Feature-Engineering (ersetze dies durch relevante Features)
features = filtered_data[['Yards', 'Week', 'Year']]
target = filtered_data['Count']

# Teile die Daten in Trainings- und Testsets auf
X_train, X_test, y_train, y_test = train_test_split(features, target, test_size=0.2, random_state=42)

# Initialisiere das lineare Regressionsmodell
model = LinearRegression()

# Trainiere das Modell
model.fit(X_train, y_train)

# Mache Vorhersagen auf dem Testset
predictions = model.predict(X_test)

# Evaluierung des Modells
mse = mean_squared_error(y_test, predictions)
print(f'Mean Squared Error: {mse}')

# Speichere das trainierte Modell
joblib.dump(model, '/opt/airflow/dags/tasks/nfl/linear_regression_model.pkl')
