from src.transform.dim_staff_maker import staff_dim
import pandas as pd
import pytest

def test_staff_dim_returns_dataframe():
    result = staff_dim()
    assert isinstance(result, pd.DataFrame)

def test_staff_dim_has_required_columns():
    result = staff_dim()
    for el in [
    "first_name", "last_name", "department_name", "location", "email_address"
        ]:
        assert el in result.columns
                
def test_data_types_are_expected():
    result = staff_dim()
    assert result["first_name"].dtype == "object"
    assert result["last_name"].dtype == "object"
    assert result["department_name"].dtype == "object"
    assert result["location"].dtype == "object"
    assert result["email_address"].dtype == "object"

        