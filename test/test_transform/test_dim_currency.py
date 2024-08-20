from src.transform.dim_currency import currency_dim
from src.transform.read_ingestion import read_ingestion
import pytest
import pandas as pd

dataframes = read_ingestion()

def test_currency_dim_returns_data_frame():
    response_df = currency_dim(dataframes['currency'])
    assert type(response_df) == pd.core.frame.DataFrame

def test_currency_dim_returns_correct_columns():
    response_df = currency_dim(dataframes['currency'])
    expected_columns = ['currency_code', 'currency_name']
    reponse_columns = list(response_df.columns.values)
    print(response_df.index)
    for column in expected_columns:
        assert column in reponse_columns
    assert response_df.index.name=='currency_id'

def test_currency_dim_returns_rows_with_correct_data_types():
    response_df = currency_dim(dataframes['currency'])
    columns = ['currency_code', 'currency_name']
    for coulumn in columns:
        assert response_df.dtypes[coulumn] == object

