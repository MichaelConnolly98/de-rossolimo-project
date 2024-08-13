from src.extract import extract
import pytest


def test_extract_function_returns_a_dictionary():
    test_func = extract()
    assert isinstance(test_func, dict)
    assert list(test_func.keys()) == ["all_data"]

def test_extract_function_returns_expected_number_of_tables():
    test_func = extract()
    assert len(test_func["all_data"]) == 11

