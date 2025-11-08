from flask import Flask, jsonify, send_from_directory
from flask_cors import CORS
import mysql.connector
import os

app = Flask(__name__, static_folder='.')
CORS(app)

db_config = {
    'host': 'localhost',
    'user': 'root',  # Cambia esto
    'password': 'annie',  # Cambia esto
    'database': 'happiness_db'
}

# Ruta para servir el dashboard HTML
@app.route('/')
def serve_dashboard():
    return send_from_directory('.', 'dashboard.html')

# Ruta de la API
@app.route('/api/predicciones')
def get_predicciones():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        cursor.execute('SELECT * FROM predicciones_happiness ORDER BY timestamp DESC')
        data = cursor.fetchall()
        cursor.close()
        conn.close()
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Ruta para verificar el estado
@app.route('/api/health')
def health_check():
    return jsonify({'status': 'OK', 'message': 'API funcionando correctamente'})

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=3000, debug=True)