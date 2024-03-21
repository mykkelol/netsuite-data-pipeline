import unittest
import pandas as pd

from pandas.testing import assert_frame_equal
from unittest.mock import patch, MagicMock
from sqlalchemy.exc import SQLAlchemyError
from transformation import transform_staged_data

class TestTransformStagedData(unittest.TestCase):
    def setUp(self):
        self.df = pd.DataFrame({
            'id': [1, 2, 3],
            'department': ['1100 Finance', '1200 People', None],
            'status': ['Pending', 'Rejected', None]
        })

    @patch('transform_staged_data.PostgresHook')
    @patch('transform_staged_data.create_engine')
    @patch('transform_staged_data.pd.read_sql')
    @patch('transform_staged_data.open')
    def test_transform_staged_data(
        self, mock_open, mock_read_sql, mock_create_engine, mock_postgres_hook
    ):
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_read_sql.return_value = self.df
        mock_conn = MagicMock()
        mock_engine.begin.return_value.__enter__.return_value = mock_conn
        mock_file = MagicMock()
        mock_file.read.return_value = 'query'
        mock_open.return_value.__enter__.return_value = mock_file
        mock_postgres_hook.return_value.get_uri.return_value = 'postgresql://username:password@localhost:5432/mydatabase'
        
        result = transform_staged_data.transform_staged_data('my_conn_id', 'my_table')
        
        self.assertTrue(result)
        
        expected_df = pd.DataFrame({
            'id': [1, 2, 3],
            'department': ['Finance', 'People', 'People'],
            'status': ['Approved', 'Pending', 'Rejected']
        })
        assert_frame_equal(expected_df, self.df)
        mock_conn.execute.assert_called()

if __name__ == '__main__':
    unittest.main()