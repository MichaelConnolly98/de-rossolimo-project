from src.transform.dim_payment_type import payment_type_dim
from src.transform.read_ingestion import read_ingestion
import pytest
import pandas as pd

dataframes = read_ingestion()

def test_payment_type_dim_returns_data_frame():
    response_df = payment_type_dim(dataframes['payment_type'])
    assert type(response_df) == pd.core.frame.DataFrame

def test_payment_type_dim_returns_correct_columns():
    response_df = payment_type_dim(dataframes['payment_type'])
    expected_columns = ['payment_type_name']
    reponse_columns = list(response_df.columns.values)
    print(response_df.index)
    assert len(expected_columns) == len(reponse_columns)
    for column in expected_columns:
        assert column in reponse_columns
    assert response_df.index.name=='payment_type_id'

def test_payment_type_dim_returns_rows_with_correct_data_types():
    response_df = payment_type_dim(dataframes['payment_type'])
    columns = ['payment_type_name']
    for coulumn in columns:
        assert response_df.dtypes[coulumn] == object