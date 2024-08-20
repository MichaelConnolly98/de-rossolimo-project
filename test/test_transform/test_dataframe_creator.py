from src.transform.pandas_testing import dataframe_creator
import pandas as pd
from unittest.mock import patch, Mock
import pytest

test_dict = {"address": [{"address_id": 1, "address_line_1": "6826 Herzog Via", "address_line_2": "null", "district": "Avon", "city": "New Patienceburgh", "postal_code": "28441", "country": "Turkey", "phone": "1803 637401", "created_at": "2022-11-03 14:20:49.962000", "last_updated": "2022-11-03 14:20:49.962000"}, {"address_id": 2, "address_line_1": "179 Alexie Cliffs", "address_line_2": "null", "district": "null", "city": "Aliso Viejo", "postal_code": "99305-7380", "country": "San Marino", "phone": "9621 880720", "created_at": "2022-11-03 14:20:49.962000", "last_updated": "2022-11-03 14:20:49.962000"}]}

def test_dataframe_creator_returns_dataframe():
    result = dataframe_creator("design")
    assert isinstance(result, pd.DataFrame)

def test_dataframe_name_is_name_of_input():
    result = dataframe_creator("design")
    assert result.name == "design"
    
def test_dataframe_creator_sets_index_to_table_name_id():
    result = dataframe_creator("design")
    assert result.index.name == "design_id"

def test_dataframe_creator_has_correct_index_without_zero():
    result = dataframe_creator("staff")
    assert 0 not in result.index
    
def test_dataframe_creator_returns_list_of_dataframes_if_no_parameter():
    result = dataframe_creator()
    for el in result:
        assert isinstance(el, pd.DataFrame)

@patch('src.transform.pandas_testing.json.load')
def test_dataframe_creator_returns_dataframes_with_expected_columns(json_load):
    json_load.return_value = test_dict
    result = dataframe_creator("address")
    for col in result.columns:
        assert col in ["address_line_1",
                    "address_line_2", 
                    "district",
                    "city",
                    "postal_code", 
                    "country", 
                    "phone", 
                    "created_at", 
                    "last_updated"]

@patch('src.transform.pandas_testing.json.load', side_effect=Exception)
def test_dataframe_creator_errors_are_logged(json_load, caplog):
    with pytest.raises(Exception):
        dataframe_creator("design")
    assert "'Result': 'Failure', 'Error': 'An exception has occured" in caplog.text
