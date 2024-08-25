from utils.final_load_dims_m import load_dim_m, load_facts_m
from utils.dim_generator import location_dim, design_dim, staff_dim,\
currency_dim, payment_type_dim, counterparty_dim, create_date_table
from utils.fact_generator import sales_facts, payment_facts, purchase_order_facts
from connection_m import local_db_connect
from run_seed_m import run_seed
import json
import pytest
from unittest.mock import patch
import sqlalchemy

engine = sqlalchemy.create_engine('postgresql+pg8000://mostyn:password@localhost:5432/test_totesys')


with open("pandas_test_data_copy.json", "r") as f:
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


    
def test_load_dims_loads_location_dim_table(seeder, conn):
    load_dim_m("dim_location", test_df, engine)
    
    
    result = conn.run("Select * from dim_location")
    assert len(result) == 30

    columns = [column["name"] for column in conn.columns]
    for col in columns:
        assert col in ['location_id', 'address_line_1', 'address_line_2', 'district', 'city', 'postal_code', 'country', 'phone']
    
def test_load_dims_loads_all_dims_tables(seeder, conn):
    # currency_df = currency_dim(file_dict)
    # payment_df = payment_type_dim(file_dict)
    # # design_df = design_dim(file_dict)
    # # staff_df = staff_dim(file_dict)
    # # counterparty_df = counterparty_dim(file_dict)
    # # # date_df = create_date_table()
    # sales_fact_df = sales_facts(file_dict)
    # # print(sales_fact_df)
    # # # payment_fact_df = payment_facts(file_dict)
    # # # purchase_fact_df = purchase_order_facts(file_dict)

    # # # load_dim_m("dim_currency", currency_df, engine)
    # # # load_dim_m("dim_payment_type", payment_df, engine)
    # # # load_dim_m("dim_design", design_df, engine)
    # # # load_dim_m("dim_staff", staff_df, engine)
    # # # load_dim_m("dim_counterparty", counterparty_df, engine)
    # # # load_dim_m("dim_date", date_df, engine)
    # load_facts_m("fact_sales_order", sales_fact_df, engine)

    # data_dict = {}
    # # # data_dict["result_currency"] = conn.run("SELECT * FROM dim_currency;")
    # # # data_dict["result_payment"] = conn.run("SELECT * FROM dim_payment_type;")
    # # # data_dict["result_design"] = conn.run("SELECT * FROM dim_design;")
    # # # data_dict["result_staff"] = conn.run("SELECT * FROM dim_staff;")
    # # # data_dict["result_counterparty"] = conn.run("SELECT * FROM dim_counterparty;")
    # # # data_dict["result_date"] = conn.run("SELECT * FROM dim_date")
    # data_dict["result_sales"] = conn.run("SELECT * FROM fact_sales_order;")
    # print(data_dict["result_sales"])

    # # # for value in data_dict.values():
    # # #     assert len(value) > 0
    pass




# #do a full test using the extract bucket and test data at some point

def test_load_dims_counterparty_erroring(seeder, conn):
    sales_fact_df = sales_facts(file_dict)
    print(sales_fact_df)
    fact_result = conn.run("SELECT * FROM fact_sales_order;")
    print(fact_result)
    result = conn.run("SELECT column_name FROM INFORMATION_SCHEMA.columns WHERE TABLE_NAME='fact_sales_order'")
    sales_df = sales_facts(file_dict)
    print(sales_df.columns)
    print(result)

[['sales_record_id'], ['sales_order_id']] # missing sales_order_id column? 
#currently breaking down on fact tables. strugglign to get any useful errors out of 
#sql alchemy
#may have to construct in pg8000 directly and see problem from there

