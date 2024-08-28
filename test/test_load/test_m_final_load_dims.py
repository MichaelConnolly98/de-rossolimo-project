from utils.final_load_dims_m import load_dim_m, load_facts_m, load_transaction
from utils.dim_generator import location_dim, design_dim, staff_dim,\
currency_dim, payment_type_dim, counterparty_dim, create_date_table, transaction_dim
from utils.fact_generator import sales_facts, payment_facts, purchase_order_facts
from utils.load_connection_m import local_db_connect
from run_seed_m import run_seed
import json
import pytest
from unittest.mock import patch
import sqlalchemy
from utils.load_connection_dbapi import connection
from utils.pandas_testing import file_data
import os
from dotenv import load_dotenv
import pandas as pd
from numpy import float64

load_dotenv()
user=os.getenv("PG_USER")
password=os.getenv("PG_PASSWORD")
database=os.getenv("PG_DATABASE")
host=os.getenv("PG_HOST")
port=int(os.getenv("PG_PORT"))

engine = sqlalchemy.create_engine(f'postgresql+pg8000://{user}:{password}@{host}:{port}/{database}')




with open("pandas_test_up_to_date.json", "r") as f:
    file_dict=json.load(f)

test_df = location_dim(file_dict=file_dict)

@pytest.fixture()
def seeder():
    run_seed()
    

@pytest.fixture(scope="function")
def conn():
    db = local_db_connect()
    yield db
    db.close()


# @pytest.mark.skip
def test_load_dims_loads_location_dim_table(seeder, conn):
    load_dim_m("dim_location", test_df, engine)
    
    
    result = conn.run("Select * from dim_location")
    assert len(result) == 30

    columns = [column["name"] for column in conn.columns]
    for col in columns:
        assert col in ['location_id', 'address_line_1', 'address_line_2', 'district', 'city', 'postal_code', 'country', 'phone']


# @pytest.mark.skip
def test_load_dims_loads_all_dims_tables(seeder, conn):
    conn_dbapi = connection()
    currency_df = currency_dim(file_dict)
    payment_df = payment_type_dim(file_dict)
    design_df = design_dim(file_dict)
    staff_df = staff_dim(file_dict)
    counterparty_df = counterparty_dim(file_dict)
    date_df = create_date_table()
    location_df = location_dim(file_dict)
    transaction_df = transaction_dim(file_dict)
    sales_fact_df = sales_facts(file_dict)
    payment_fact_df = payment_facts(file_dict)
    purchase_order_fact_df = purchase_order_facts(file_dict)
   
    load_dim_m("dim_currency", currency_df, engine)
    load_dim_m("dim_payment_type", payment_df, engine)
    load_dim_m("dim_design", design_df, engine)
    load_dim_m("dim_staff", staff_df, engine)
    load_dim_m("dim_counterparty", counterparty_df, engine)
    load_dim_m("dim_date", date_df, engine)
    load_dim_m("dim_location", location_df, engine)
    load_transaction(transaction_df, conn_dbapi)
    load_facts_m("fact_sales_order", sales_fact_df, engine)
    load_facts_m("fact_payment", payment_fact_df, engine)
    load_facts_m("fact_purchase_order",purchase_order_fact_df, engine)

    data_dict = {}
    data_dict["result_currency"] = conn.run("SELECT * FROM dim_currency;")
    data_dict["result_payment"] = conn.run("SELECT * FROM dim_payment_type;")
    data_dict["result_design"] = conn.run("SELECT * FROM dim_design;")
    data_dict["result_staff"] = conn.run("SELECT * FROM dim_staff;")
    data_dict["result_counterparty"] = conn.run("SELECT * FROM dim_counterparty;")
    data_dict["result_date"] = conn.run("SELECT * FROM dim_date")
    data_dict["result_location"] = conn.run("SELECT * FROM dim_location")
    data_dict["result_transaction"] = conn.run("SELECT * FROM dim_transaction;")
    data_dict["result_fact_sales"] = conn.run("SELECT * FROM fact_sales_order;")
    data_dict["result_fact_payment"] = conn.run("SELECT * FROM fact_payment;")
    data_dict["result_fact_purchase"] = conn.run("SELECT * FROM fact_purchase_order;")
    

    for value in data_dict.values():
        assert len(value) > 0


def test_load_transation_loads_correctly(seeder):
    conn_dbapi = connection()
    transaction_df = transaction_dim(file_dict)
    load_transaction(transaction_df, conn_dbapi)
    sql_query = "SELECT * FROM dim_transaction;"
    response = conn_dbapi.run(sql_query)
    read_df = pd.read_sql(sql_query, engine)
    expected_columns = ['transaction_id', 'transaction_type', 'sales_order_id', 'purchase_order_id']
    columns = list(read_df.columns.values)
    assert set(expected_columns) == set(columns)
    assert len(expected_columns) == len(columns)
    assert read_df['transaction_id'].iloc[0] == 1
    assert read_df['transaction_type'].iloc[0] == 'PURCHASE'
    assert type(read_df['sales_order_id'].iloc[0]) == float64
    assert read_df['purchase_order_id'].iloc[0] == 2.0


#do a full test using the extract bucket and test data at some point

# def test_load_dims_counterparty_erroring(seeder, conn):
#     purchase_order_df = purchase_order_facts(file_dict)
#     print(purchase_order_df.head().to_string())
#     fact_result = conn.run("SELECT * FROM fact_purchase_order;")
#     print(fact_result)
#     result = conn.run("SELECT column_name FROM INFORMATION_SCHEMA.columns WHERE TABLE_NAME='fact_purchase_order'")


# # [['sales_record_id'], ['sales_order_id']] # missing sales_order_id column? 
# # # # #currently breaking down on fact tables. strugglign to get any useful errors out of 
# # # #sql alchemy
# # # #may have to construct in pg8000 directly and see problem from there

# # # #ok so getting a problem with sales order id being put in to the dataframe in the first place
# # # #saying no to duplicates
# # # #why is it a duplicate in the first place?? 


