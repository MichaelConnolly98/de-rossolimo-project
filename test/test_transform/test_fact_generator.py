from utils.fact_generator import sales_facts, purchase_order_facts, payment_facts
import pandas as pd
import json

with open("pandas_test_data_copy.json", "r") as f:
    file_dict=json.load(f)

class TestSalesFact:
    def test_sales_facts_returns_data_frame(self):
        response_df = sales_facts(file_dict=file_dict)
        assert type(response_df) == pd.core.frame.DataFrame

    def test_sales_facts_returns_correct_columns(self):
        response_df = sales_facts(file_dict=file_dict)
        expected_columns = [
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
        assert response_df.index.name=='sales_order_id'
        assert len(response_columns) == len(expected_columns)

    def test_sales_facts_returns_rows_with_correct_data_types(self):
        response_df = sales_facts(file_dict=file_dict)
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

class TestPurchaseOrderFact:
    def test_purchase_order_facts_returns_data_frame(self):
        response_df = purchase_order_facts(file_dict=file_dict)
        assert type(response_df) == pd.core.frame.DataFrame

    def test_purchase_order_facts_returns_correct_columns(self):
        response_df = purchase_order_facts(file_dict=file_dict)
        expected_columns = [
            "created_date", 
            "created_time", 
            "last_updated_date", 
            "last_updated_time", 
            "staff_id", 
            "counterparty_id", 
            "item_code", 
            "item_quantity", 
            "item_unit_price", 
            "currency_id", 
            "agreed_delivery_date",
            "agreed_payment_date", 
            "agreed_delivery_location_id"
        ]

        response_columns = list(response_df.columns.values)
        for column in expected_columns:
            assert column in response_columns
        assert response_df.index.name=='purchase_order_id'
        assert len(response_columns) == len(expected_columns)

    def test_purchase_order_facts_returns_rows_with_correct_data_types(self):
        response_df = purchase_order_facts(file_dict=file_dict)
        assert response_df.dtypes['created_date'] == '<M8[ns]'
        assert response_df.dtypes['created_time'] == object
        assert response_df.dtypes['last_updated_date'] == '<M8[ns]'
        assert response_df.dtypes['last_updated_time'] == object
        assert response_df.dtypes['staff_id'] == 'int64'
        assert response_df.dtypes['counterparty_id'] == 'int64'
        assert response_df.dtypes['item_code'] ==  object
        assert response_df.dtypes['item_quantity'] == 'int64'
        assert response_df.dtypes['item_unit_price'] == 'float64'
        assert response_df.dtypes['currency_id'] == 'int64'
        assert response_df.dtypes['agreed_delivery_date'] == '<M8[ns]'
        assert response_df.dtypes['agreed_payment_date'] == '<M8[ns]'
        assert response_df.dtypes['agreed_delivery_location_id'] == 'int64'

class TestPaymentFact:
    def test_payment_facts_returns_data_frame(self):
        response_df = payment_facts(file_dict=file_dict)
        assert type(response_df) == pd.core.frame.DataFrame

    def test_payment_facts_returns_correct_columns(self):
        response_df = payment_facts(file_dict=file_dict)
        expected_columns = [
        "created_date", 
        "created_time", 
        "last_updated_date", 
        "last_updated_time", 
        "transaction_id", 
        "counterparty_id", 
        "payment_amount",
        "currency_id",
        "payment_type_id",
        "paid", 
        "payment_date"
    ]


        response_columns = list(response_df.columns.values)
        for column in expected_columns:
            assert column in response_columns
        assert response_df.index.name=='payment_id'
        assert len(response_columns) == len(expected_columns)

    def test_payment_facts_returns_rows_with_correct_data_types(self):
        response_df = payment_facts(file_dict=file_dict)
        assert response_df.dtypes['created_date'] == '<M8[ns]'
        assert response_df.dtypes['created_time'] == object
        assert response_df.dtypes['last_updated_date'] == '<M8[ns]'
        assert response_df.dtypes['last_updated_time'] == object
        assert response_df.dtypes['transaction_id'] == 'int64'
        assert response_df.dtypes['counterparty_id'] == 'int64'
        assert response_df.dtypes['payment_amount'] ==  'float64'
        assert response_df.dtypes['currency_id'] == 'int64'
        assert response_df.dtypes['payment_type_id'] == 'int64'
        assert response_df.dtypes['paid'] == 'bool'
        assert response_df.dtypes['payment_date'] == '<M8[ns]'