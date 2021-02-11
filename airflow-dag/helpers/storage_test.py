import unittest
from unittest.mock import patch, mock_open
import glob
from datetime import datetime
import os

import storage

# START TESTS: store module
class Test(unittest.TestCase):
    path_to_open = 'file/path/file.xslx'

    def test_open_file(self):
        with patch('builtins.open', mock_open(read_data='data'), create=True) as m:
            self.assertEqual(storage.open_file(self.path_to_open).read(), 'data')
            m.assert_called_with(self.path_to_open, 'rb')

    def test_get_conn(self):
        self.assertEqual(storage.get_conn().find('None'), -1)

unittest.main()