# test_job.py

import unittest
import os
import shutil
from job import Job
from unittest.mock import patch, MagicMock

class TestJob(unittest.TestCase):
    def setUp(self):
        # Налаштування параметрів для тесту
        self.date = "2022-08-09"
        self.feature = "gold"
        self.raw_dir = "/tmp/test_raw/gold/2022-08-09"
        self.job = Job(date=self.date, feature=self.feature, raw_dir=self.raw_dir)
        
        # Створюємо директорію, якщо її немає
        os.makedirs(self.raw_dir, exist_ok=True)

    def tearDown(self):
        # Видаляємо тимчасові файли після тесту
        if os.path.exists(self.raw_dir):
            shutil.rmtree(self.raw_dir)

    @patch('job.Job.fetch_sales_data')
    def test_run_clears_directory_and_saves_data(self, mock_fetch_sales_data):
        mock_data = [{"sale_id": 1, "amount": 100}, {"sale_id": 2, "amount": 150}]
        mock_fetch_sales_data.return_value = mock_data
        
        # Створюємо тимчасовий файл для перевірки очищення
        with open(os.path.join(self.raw_dir, "temp_file.txt"), "w") as f:
            f.write("temp data")
        
        # Виконуємо метод run
        self.job.run()
        
        # Перевірка, що директорія очищена
        self.assertFalse(os.listdir(self.raw_dir), "Directory should be empty after clearing.")
        
        # Перевірка, що дані зберігаються правильно
        expected_file_path = os.path.join(self.raw_dir, f"{self.date}.json")
        self.assertTrue(os.path.exists(expected_file_path), "Expected JSON file was not created.")
        
        with open(expected_file_path) as f:
            saved_data = json.load(f)
            self.assertEqual(saved_data, mock_data)

    @patch('firebase_admin.firestore.client')
    @patch('firebase_admin.credentials.Certificate')
    def test_initialize_firebase(self, mock_certificate, mock_client):
        # Перевіряємо, чи ініціалізація Firebase проходить один раз
        self.job.initialize_firebase()
        self.assertTrue(self.job.firebase_initialized, "Firebase should be initialized.")
        mock_certificate.assert_called_once()
        mock_client.assert_called_once()

if __name__ == '__main__':
    unittest.main()
