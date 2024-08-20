from src.transform.dim_location import location_dim
import pandas as pd
import pytest


def test_location_dim_returns_dataframe():
    result = location_dim()
    assert isinstance(result, pd.DataFrame)

def test_location_dim_has_required_columns():
    result = location_dim()
    for el in [
    "location", "department_name"
        ]:
        assert el in result.columns
                
def test_location_dim_data_types_are_expected():
    result = location_dim()
    assert result["location"].dtype == "object"
    assert result["department_name"].dtype == "object"