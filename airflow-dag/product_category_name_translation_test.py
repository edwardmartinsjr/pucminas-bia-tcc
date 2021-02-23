import unittest
from unittest.mock import patch, mock_open
import glob
from datetime import datetime
import os

import product_category_name_translation as task
from helpers import storage

# START TESTS:
class Test(unittest.TestCase):
    db_name = os.getenv('MYSQL_NAME')
    table_name = 'product_category_name_translation'
    file_full_path = './airflow-dag/dummy_files/'+table_name+'.csv'

    def test_clear_db_func(self):
        self.assertEqual(task.clear_db_func(self.db_name+'.'+self.table_name),True)

    def test_etl(self):
        df = storage.extract_data_from_csv(self.file_full_path, self.db_name+'.'+self.table_name)
        self.assertEqual(len(df.index),11)
        df = task.transform_data_func(df)
        self.assertEqual(len(df.index),10)
        self.assertEqual(storage.load_data_into_db(df, self.db_name, self.table_name),True)
        

unittest.main()