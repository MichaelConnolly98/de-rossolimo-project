from src.transform.dim_design import design_dim
import pandas as pd
import pytest


def test_design_dim_returns_dataframe():
    result = design_dim()
    assert isinstance(result, pd.DataFrame)

def test_design_dim_has_required_columns():
    result = design_dim()
    for el in [
    "design_name", "file_location", "file_name"
        ]:
        assert el in result.columns
                
def test_design_dim_data_types_are_expected():
    result = design_dim()
    assert result["design_name"].dtype == "object"
    assert result["file_location"].dtype == "object"
    assert result["file_name"].dtype == "object"