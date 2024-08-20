from src.transform.dim_counterparty import counterparty_dim
import pandas as pd
import pytest

def test_counterparty_dim_returns_dataframe():
    result = counterparty_dim()
    assert isinstance(result, pd.DataFrame)

def test_counterparty_dim_has_required_columns():
    result = counterparty_dim()
    for el in [
    "counterparty_legal_address_line_1",
    "counterparty_legal_address_line_2",
    "counterparty_legal_district", 
    "counterparty_legal_city", 
    "counterparty_legal_country",
    "counterparty_legal_phone_number",
    "counterparty_legal_postal_code"
        ]:
        assert el in result.columns
                
def test_counterparty_data_types_are_expected():
    result = counterparty_dim()
    assert result["counterparty_legal_address_line_2"].dtype == "object"
    assert result["counterparty_legal_address_line_1"].dtype == "object"
    assert result["counterparty_legal_district"].dtype == "object"
    assert result["counterparty_legal_city"].dtype == "object"
    assert result["counterparty_legal_country"].dtype == "object"
    assert result["counterparty_legal_phone_number"].dtype == "object"
    assert result["counterparty_legal_postal_code"].dtype == "object"

def test_counterparty_index_is_expected():
    result = counterparty_dim()
    assert result.index.name == "counterparty_id"


        