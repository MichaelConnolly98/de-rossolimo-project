from src.transform.extract_bucket import lambda_handler
import pytest
import json
from unittest.mock import patch
import pandas as pd
from botocore.exceptions import ClientError



@pytest.fixture()
def test_file_dict():
    with open("pandas_test_data_copy.json") as f:
        with patch(
            "src.transform.extract_bucket.file_data_single",
            return_value=json.load(f)) as file_dict:
            yield file_dict

def test_lambda_handler_returns_dataframes_as_values(test_file_dict):
    result = lambda_handler(0, 0)
    for d in result.values():
        assert isinstance(d, pd.DataFrame)
    for i in [
            "currency_table", 
            "payment_dim",
            "staff_dim",
            "counterparty_dim",
            "location_dim",
            "design_dim",
            "sales_facts",
            "payment_facts",
            "purchase_order_facts"]:
        assert i in result.keys()

@patch("src.transform.extract_bucket.file_data_single", return_value={"":[]})
def test_lambda_handler_returns_key_error_if_empty_data_passed_in(test_file_none, caplog):
    with pytest.raises(KeyError):
        lambda_handler(0,0)
    assert "Missing required keys" in caplog.text

@patch("src.transform.extract_bucket.file_data_single")
@patch("src.transform.extract_bucket.create_date_table", return_value=None)
def test_lambda_handler_returns_None_if_keys_present_but_data_not(
    date_none ,test_file_empty, caplog
    ):
    test_file_empty.return_value = {"address": [], 
                                    "currency": [],
                                    "payment" : [],
                                    "staff" : [],
                                    "counterparty" : [],
                                    "address": [],
                                    "design" : [],
                                    "sales_order" : [],
                                    "payment_type" : [],
                                    "purchase_order": []}
    result = lambda_handler(0, 0)
    assert not all(result.values())
    assert "No dataframes created" in caplog.text

@patch("src.transform.extract_bucket.file_data_single", side_effect=Exception)
def test_lambda_handler_catches_and_logs_errors(test_file_error, caplog):
    with pytest.raises(Exception):
        lambda_handler(0, 0)
    assert "Exception occured" in caplog.text

@patch("src.transform.extract_bucket.create_date_table")
@patch("src.transform.extract_bucket.currency_dim")
@patch("src.transform.extract_bucket.payment_type_dim")
@patch("src.transform.extract_bucket.staff_dim")
@patch("src.transform.extract_bucket.counterparty_dim")
@patch("src.transform.extract_bucket.location_dim")
@patch("src.transform.extract_bucket.design_dim")
@patch("src.transform.extract_bucket.sales_facts")
@patch("src.transform.extract_bucket.payment_facts")
@patch("src.transform.extract_bucket.purchase_order_facts")
def test_lambda_handler_invokes_all_dataframe_creater_subfunctions(
    po, pf, sf, de, lo, cp, st, pt, cu, cd, test_file_dict
    ):
    lambda_handler(0,0)
    po.assert_called_once()
    pf.assert_called_once()
    sf.assert_called_once()
    cp.assert_called_once()
    st.assert_called_once()
    pt.assert_called_once()
    cu.assert_called_once()
    cd.assert_called_once()
    lo.assert_called_once()
    de.assert_called_once()
    
    
