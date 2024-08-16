from src.extract.extract_data import extract
from datetime import datetime
import pytest
from pg8000.native import DatabaseError
import logging
from unittest.mock import patch


def test_extract_function_returns_a_dictionary():
    test_func = extract()
    assert isinstance(test_func, dict)
    assert list(test_func.keys()) == ["all_data"]

def test_extract_function_returns_expected_number_of_tables():
    test_func = extract()
    assert len(test_func["all_data"]) == 11

def test_extract_function_returns_table_names_if_there_are_no_rows_of_data():
    datetime_str = "9999-12-31 23:59:59.999"
    test_func = extract(datetime_str)
    assert len(list((test_func['all_data']).keys())) == 11
    assert len(test_func['all_data']["sales_order"]) == 0

def test_extract_with_datetime_filter_applied_returns_filtered_data():
    datetime_str_format = "2023-12-31 23:59:59.999"
    datetime_date_format = datetime.fromisoformat(datetime_str_format)
    test_func = extract(datetime_str_format)
    for data in test_func["all_data"]["sales_order"]:
        assert data["last_updated"] > datetime_date_format

def test_database_error_raised_when_invalid_argument_given_to_extract_function(
    caplog,):
    with caplog.at_level(logging.ERROR):
        with pytest.raises(DatabaseError) as e:
            extract("staff")
        assert str(e.value) == "A database error has occured"
        assert "A database error has occured" in caplog.text

@patch("src.extract.extract_data.get_connection", side_effect=Exception)
def test_error_is_raised_when_exception_occurs(mock_connection, caplog):
    with caplog.at_level(logging.ERROR):
        with pytest.raises(Exception) as e:
            extract()
        assert str(e.value) == "An error has occured"
        assert "An error has occured" in caplog.text