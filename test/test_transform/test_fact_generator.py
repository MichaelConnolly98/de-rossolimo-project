from src.transform.fact_generator import sales_facts
import pandas as pd

class TestSalesFact:
    def test_currency_dim_returns_data_frame(self):
        response_df = sales_facts()
        assert type(response_df) == pd.core.frame.DataFrame

    def test_currency_dim_returns_correct_columns(self):
        response_df = sales_facts()
        expected_columns = [
            "sales_order_id", 
            "created_date", 
            "created_time", 
            "last_updated_date", 
            "last_updated_time", 
            "sales_staff_id", 
            "counterparty_id", 
            "units_sold", 
            "unit_price", 
            "currency_id", 
            "design_id", 
            "agreed_payment_date", 
            "agreed_delivery_date", 
            "agreed_delivery_location_id"
            ]
        response_columns = list(response_df.columns.values)
        for column in expected_columns:
            assert column in response_columns
        assert response_df.index.name=='sales_record_id'
        assert len(response_columns) == len(expected_columns)

    def test_currency_dim_returns_rows_with_correct_data_types(self):
        response_df = sales_facts()
        assert response_df.dtypes['sales_order_id'] == 'int64'
        assert response_df.dtypes['created_date'] == '<M8[ns]'
        assert response_df.dtypes['created_time'] == object
        assert response_df.dtypes['last_updated_date'] == '<M8[ns]'
        assert response_df.dtypes['last_updated_time'] == object
        assert response_df.dtypes['sales_staff_id'] == 'int64'
        assert response_df.dtypes['counterparty_id'] == 'int64'
        assert response_df.dtypes['units_sold'] == 'int64'
        assert response_df.dtypes['unit_price'] == 'float64'
        assert response_df.dtypes['currency_id'] == 'int64'
        assert response_df.dtypes['design_id'] == 'int64'
        assert response_df.dtypes['agreed_payment_date'] == '<M8[ns]'
        assert response_df.dtypes['agreed_delivery_date'] == '<M8[ns]'
        assert response_df.dtypes['agreed_delivery_location_id'] == 'int64'
