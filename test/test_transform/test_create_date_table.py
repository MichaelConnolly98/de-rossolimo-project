from src.transform.dim_date import create_date_table
import pandas as pd

def test_create_date_table_returns_dataframe():
    result = create_date_table()
    assert isinstance(result, pd.DataFrame)

def test_create_date_table_has_required_columns():
    result = create_date_table()
    for el in [
    "year", "month", "day", "day_of_week", "day_name", "month_name", "quarter"
        ]:
        assert el in result.columns

def test_create_date_table_has_expected_value_for_year_rows():
    result = create_date_table()
    for el in [
    "year", "month", "day", "day_of_week", "day_name", "month_name", "quarter"
        ]:
        col = result.loc[:, el]
        if col.name == "year":
            for i in range(2000, 2051):
                assert i in col.values
        if col.name == "month":
            for i in range(1, 13):
                assert i in col.values
        if col.name == "day":
            for i in range(1, 5):
                assert i in col.values
                
def test_data_types_are_expected():
    result = create_date_table()
    assert result["year"].dtype == "int32"
    assert result["month"].dtype == "int32"
    assert result["day"].dtype == "int32"
    assert result["day_of_week"].dtype == "int32"
    assert result["day_name"].dtype == "object"
    assert result["month_name"].dtype == "object"
    assert result["quarter"].dtype == "int32"
        