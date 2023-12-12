from flask import Flask, jsonify, request

import joblib
import pandas as pd

app = Flask(__name__)

@app.route('/predict', methods=['POST'])
def predict():
    try:
        # Lese die Daten aus dem JSON-Format im Request-Body
        input_data = request.json

        # Überprüfe, ob die erforderlichen Schlüssel im Input vorhanden sind
        required_keys = ['Yards', 'Week', 'Year']
        if not all(key in input_data for key in required_keys):
            return jsonify({'error': 'Missing required keys in input data'}), 400

        # Konvertiere die Daten in ein DataFrame
        new_data = pd.DataFrame({
            'Yards': [input_data['Yards']],
            'Week': [input_data['Week']],
            'Year': [input_data['Year']]
        })

        # Lade das Modell und mache Vorhersagen auf den neuen Daten
        loaded_model = joblib.load('/opt/airflow/dags/tasks/nfl/linear_regression_model.pkl')
        predictions = loaded_model.predict(new_data).tolist()

        return jsonify({'prediction': predictions})
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)
