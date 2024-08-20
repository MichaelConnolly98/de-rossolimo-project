from src.transform.pandas_date_table_practice import create_date_table
import pandas as pd

def test_create_date_table_returns_dataframe():
    result = create_date_table()
    assert isinstance(result, pd.DataFrame)

def test_create_date_table_has_required_columns():
    result = create_date_table()
    print(result.columns)
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
        
                
        