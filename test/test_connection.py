from src.extract.extract_data import get_connection
from pg8000.native import Connection, DatabaseError, InterfaceError
from unittest.mock import patch
import pytest

def test_get_connection_returns_connection_object():
    result = get_connection()
    assert isinstance(result, Connection)

@patch('src.extract.extract_data.get_db_credentials')
def test_get_connection_catches_and_logs_errors(mock_creds, caplog):
    mock_creds.return_value = {"username": "nothing",
                               "password" : "nothing",
                               "dbname": "nothing",
                               "host": "nothin",
                               "port": 5432}
    with pytest.raises(InterfaceError):
        get_connection()
        assert "A database connection error has occured" in caplog.text
   
@patch('src.extract.extract_data.get_db_credentials')
def test_get_connection_catches_general_exceptions(mock_creds, caplog):
    mock_creds.side_effect = Exception
    with pytest.raises(Exception):
        get_connection()
        assert "A database connection error has occured" in caplog.text

def test_no_logs_on_success(caplog):
    get_connection()
    assert not caplog.text
   