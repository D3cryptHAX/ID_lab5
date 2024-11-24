# main.py

from flask import Flask, request, jsonify
import os
from job import Job

app = Flask(__name__)

# Запуск на локальному сервері
@app.route('/fetch_data', methods=['POST'])
def fetch_data():
    data = request.get_json()
    date = data.get("date")
    feature = data.get("feature")
    
    if not date or not feature:
        return jsonify({"error": "Date and feature are required"}), 400
    
    # Шлях для зберігання файлів
    raw_dir = os.path.join("path/to/my_dir", "raw", feature, date)
    
    # Ініціалізація та запуск Job
    job = Job(date=date, feature=feature, raw_dir=raw_dir)
    job.run()
    
    return jsonify({"status": "Data fetched successfully"}), 200

if __name__ == '__main__':
    app.run(port=8081)
