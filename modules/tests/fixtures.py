import pytest

from transformation import transform_staged_data

@pytest.fixture
def transform_staged_data():
    yield transform_staged_data('my_conn_id', 'my_table')