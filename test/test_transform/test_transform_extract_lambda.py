from utils.transformer import lambda_transformer
import pytest
import json
from unittest.mock import patch
import pandas as pd
from botocore.exceptions import ClientError



@pytest.fixture()
def test_file_dict():
    with open("pandas_test_data_copy.json") as f:
        with patch(
            "utils.transformer.file_data_single",
            return_value=json.load(f)) as file_dict:
            yield file_dict

def test_lambda_transformer_returns_dataframes_as_values(test_file_dict):
    result = lambda_transformer()
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

@patch("utils.transformer.file_data_single", return_value={"":[]})
def test_lambda_transformer_returns_key_error_if_empty_data_passed_in(test_file_none, caplog):
    with pytest.raises(KeyError):
        lambda_transformer()
    assert "Missing required keys" in caplog.text

@patch("utils.transformer.file_data_single")
@patch("utils.transformer.create_date_table", return_value=None)
def test_lambda_transformer_returns_None_if_keys_present_but_data_not(
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
    result = lambda_transformer()
    assert not all(result.values())
    assert "No dataframes created" in caplog.text

@patch("utils.transformer.file_data_single", side_effect=Exception)
def test_lambda_transformer_catches_and_logs_errors(test_file_error, caplog):
    with pytest.raises(Exception):
        lambda_transformer()
    assert "Exception occured" in caplog.text

@patch("utils.transformer.create_date_table")
@patch("utils.transformer.currency_dim")
@patch("utils.transformer.payment_type_dim")
@patch("utils.transformer.staff_dim")
@patch("utils.transformer.counterparty_dim")
@patch("utils.transformer.location_dim")
@patch("utils.transformer.design_dim")
@patch("utils.transformer.sales_facts")
@patch("utils.transformer.payment_facts")
@patch("utils.transformer.purchase_order_facts")
def test_lambda_transformer_invokes_all_dataframe_creater_subfunctions(
    po, pf, sf, de, lo, cp, st, pt, cu, cd, test_file_dict
    ):
    lambda_transformer()
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
    
    
