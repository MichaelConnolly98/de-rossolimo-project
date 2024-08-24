from utils.final_load_dims_m import load_dim_m
from utils.dim_generator import location_dim
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

# @pytest.fixture(scope="function")
# def conn():
#     dbc = local_db_connect()
#     return dbc


    
def test_load_dims_loads_location_dim_table(seeder):
    conn = local_db_connect()
    load_dim_m("dim_location", test_df, engine)
    
    
    result = conn.run("Select * from dim_location")
    assert len(result) == 30

    columns = [column["name"] for column in conn.columns]
    for col in columns:
        assert col in ['location_id', 'address_line_1', 'address_line_2', 'district', 'city', 'postal_code', 'country', 'phone']
    conn.close()

#really struggling with connection being closed here, and seeder not
#reopining it. May have to be more explicit

# def test_load_dims_loads_all_dims_tables(seeder):
#     pass

# #do a full test using the extract bucket and test data at some point


