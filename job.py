# job.py

import os
import json
import firebase_admin
from firebase_admin import credentials, firestore

class Job:
    def __init__(self, date, feature, raw_dir):
        self.date = date
        self.feature = feature
        self.raw_dir = raw_dir
        self.firebase_initialized = False

    def initialize_firebase(self):
        if not self.firebase_initialized:
            cred = credentials.Certificate("path/to/your_firebase_credentials.json")
            firebase_admin.initialize_app(cred)
            self.db = firestore.client()
            self.firebase_initialized = True

    def fetch_sales_data(self):
        self.initialize_firebase()
        # Запит для отримання всіх даних продажів з Firebase
        sales_data = []
        sales_ref = self.db.collection('sales').where("feature", "==", self.feature)
        docs = sales_ref.stream()
        for doc in docs:
            sales_data.append(doc.to_dict())
        return sales_data

    def clear_raw_directory(self):
        if os.path.exists(self.raw_dir):
            for filename in os.listdir(self.raw_dir):
                file_path = os.path.join(self.raw_dir, filename)
                os.unlink(file_path)
        else:
            os.makedirs(self.raw_dir)

    def save_data_to_file(self, sales_data):
        file_path = os.path.join(self.raw_dir, f"{self.date}.json")
        with open(file_path, 'w') as f:
            json.dump(sales_data, f, indent=4)

    def run(self):
        self.clear_raw_directory()
        sales_data = self.fetch_sales_data()
        self.save_data_to_file(sales_data)
