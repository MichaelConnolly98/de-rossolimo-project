from src.extract import extract
from datetime import datetime
import pytest

def test_extract_function_returns_a_dictionary():
    test_func = extract()
    assert isinstance(test_func, dict)
    assert list(test_func.keys()) == ["all_data"]

def test_extract_function_returns_expected_number_of_tables():
    test_func = extract()
    assert len(test_func["all_data"]) == 11

def test_extract_with_datetime_filter_applied_returns_filtered_data():
    datetime_str_format = '2023-12-31 23:59:59.999'
    datetime_date_format = datetime.fromisoformat(datetime_str_format)
    test_func = extract(datetime_str_format)
    for data in test_func['all_data']['sales_order']:
        assert data['last_updated'] > datetime_date_format



