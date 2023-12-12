from flask import Flask, jsonify
import joblib
import pandas as pd

app = Flask(__name__)

@app.route('/predict', methods=['POST'])
def predict():
    loaded_model = joblib.load('/opt/airflow/dags/tasks/nfl/linear_regression_model.pkl')

    # Annahme: Du hast neue Daten, auf die du Vorhersagen machen möchtest
    new_data = pd.DataFrame({
        'Yards': [56, 100, 23],
        'Week': [1, 2, 3],
        'Year': [2023, 2023, 2023]
    })

    # Mache Vorhersagen auf den neuen Daten
    predictions = loaded_model.predict(new_data)

    return jsonify({'prediction': predictions})

if __name__ == '__main__':
    app.run(debug=True)
