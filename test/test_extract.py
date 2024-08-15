from src.extract.extract import extract
from datetime import datetime
import pytest
from pg8000.native import DatabaseError
import logging
from unittest.mock import patch

@pytest.mark.skip()
def test_extract_function_returns_a_dictionary():
    test_func = extract()
    assert isinstance(test_func, dict)
    assert list(test_func.keys()) == ["all_data"]

@pytest.mark.skip()
def test_extract_function_returns_expected_number_of_tables():
    test_func = extract()
    assert len(test_func["all_data"]) == 11

@pytest.mark.skip()
def test_extract_with_datetime_filter_applied_returns_filtered_data():
    datetime_str_format = '2023-12-31 23:59:59.999'
    datetime_date_format = datetime.fromisoformat(datetime_str_format)
    test_func = extract(datetime_str_format)
    for data in test_func['all_data']['sales_order']:
        assert data['last_updated'] > datetime_date_format


def test_database_error_is_raised_when_unacceptable_argument_is_given_to_extract_function(caplog):
    with caplog.at_level(logging.ERROR):
    
        with pytest.raises(DatabaseError) as e:
            result = extract( "staff")
            assert str(e.value) == "A database error has occured"
            assert "A database error has occured" in caplog.text


@patch("src.extract.extract.get_connection", side_effect = Exception)
def test_error_is_raised_when_exception_occurs(caplog):
    with caplog.at_level(logging.ERROR):
    
        with pytest.raises(Exception) as e:
            result = extract()
            assert str(e.value) == "An error has occured"
            assert "An error has occured" in caplog.text







