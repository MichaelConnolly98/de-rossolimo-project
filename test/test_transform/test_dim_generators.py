from src.transform.dim_generator import create_date_table
import pandas as pd
from src.transform.dim_generator import currency_dim
from src.transform.read_ingestion import read_ingestion_function
from src.transform.dim_generator import counterparty_dim
from src.transform.dim_generator import payment_type_dim
from src.transform.dim_generator import staff_dim
import json

with open("pandas_test_data_copy.json", "r") as f:
    file_dict=json.load(f)

class TestCreateDate:
    def test_create_date_table_returns_dataframe(self):
        result = create_date_table()
        assert isinstance(result, pd.DataFrame)

    def test_create_date_table_has_required_columns(self):
        result = create_date_table()
        for el in [
        "year", "month", "day", "day_of_week", "day_name", "month_name", "quarter"
            ]:
            assert el in result.columns

    def test_create_date_table_has_expected_value_for_year_rows(self):
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
                    
    def test_data_types_are_expected(self):
        result = create_date_table()
        assert result["year"].dtype == "int32"
        assert result["month"].dtype == "int32"
        assert result["day"].dtype == "int32"
        assert result["day_of_week"].dtype == "int32"
        assert result["day_name"].dtype == "object"
        assert result["month_name"].dtype == "object"
        assert result["quarter"].dtype == "int32"
            
class TestCurrencyDim:

   
    def test_currency_dim_returns_data_frame(self):
        response_df = currency_dim(file_dict=file_dict)
        assert type(response_df) == pd.core.frame.DataFrame

    def test_currency_dim_returns_correct_columns(self):
        response_df = currency_dim(file_dict=file_dict)
        expected_columns = ['currency_code', 'currency_name']
        response_columns = list(response_df.columns.values)
        print(response_df.index)
        for column in expected_columns:
            assert column in response_columns
        assert response_df.index.name=='currency_id'
        assert len(response_columns) == len(expected_columns)


    def test_currency_dim_returns_rows_with_correct_data_types(self):
        response_df = currency_dim(file_dict=file_dict)
        columns = ['currency_code', 'currency_name']
        for coulumn in columns:
            assert response_df.dtypes[coulumn] == object

class TestDimCounterparty:


    def test_counterparty_dim_returns_dataframe(self):
        result = counterparty_dim(file_dict=file_dict)
        assert isinstance(result, pd.DataFrame)

    def test_counterparty_dim_has_required_columns(self):
        result = counterparty_dim(file_dict=file_dict)
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
                    
    def test_counterparty_data_types_are_expected(self):
        result = counterparty_dim(file_dict=file_dict)
        assert result["counterparty_legal_address_line_2"].dtype == "object"
        assert result["counterparty_legal_address_line_1"].dtype == "object"
        assert result["counterparty_legal_district"].dtype == "object"
        assert result["counterparty_legal_city"].dtype == "object"
        assert result["counterparty_legal_country"].dtype == "object"
        assert result["counterparty_legal_phone_number"].dtype == "object"
        assert result["counterparty_legal_postal_code"].dtype == "object"

    def test_counterparty_index_is_expected(self):
        result = counterparty_dim(file_dict=file_dict)
        assert result.index.name == "counterparty_id"

class TestDimPayment:


    def test_payment_type_dim_returns_data_frame(self):
        response_df = payment_type_dim(file_dict=file_dict)
        assert type(response_df) == pd.core.frame.DataFrame

    def test_payment_type_dim_returns_correct_columns(self):
        response_df = payment_type_dim(file_dict=file_dict)
        expected_columns = ['payment_type_name']
        reponse_columns = list(response_df.columns.values)
        assert len(expected_columns) == len(reponse_columns)
        for column in expected_columns:
            assert column in reponse_columns
        assert response_df.index.name=='payment_type_id'

    def test_payment_type_dim_returns_rows_with_correct_data_types(self):
        response_df = payment_type_dim(file_dict=file_dict)
        columns = ['payment_type_name']
        for coulumn in columns:
            assert response_df.dtypes[coulumn] == object

class TestDimStaff:


    def test_staff_dim_returns_dataframe(self):
        result = staff_dim(file_dict=file_dict)
        assert isinstance(result, pd.DataFrame)

    def test_staff_dim_has_required_columns(self):
        result = staff_dim(file_dict=file_dict)
        for el in [
        "first_name", "last_name", "department_name", "location", "email_address"
            ]:
            assert el in result.columns
                    
    def test_staff_dim_data_types_are_expected(self):
        result = staff_dim(file_dict=file_dict)
        assert result["first_name"].dtype == "object"
        assert result["last_name"].dtype == "object"
        assert result["department_name"].dtype == "object"
        assert result["location"].dtype == "object"
        assert result["email_address"].dtype == "object"

    def test_staff_dim_index_is_expected(self):
        result = staff_dim(file_dict=file_dict)
        assert result.index.name == "staff_id"

            