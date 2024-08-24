from utils.final_load_dims import load_dims
from utils.dim_generator import location_dim
from connection_m import db, local_db_connect
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

@pytest.fixture()
def get_connection_patch():
    with patch("utils.final_load_dims.get_connection", return_value=db):
        yield 
    
def test_load_dims(seeder):
    load_dims("dim_location", test_df, engine)
    
    
    result = db.run("Select * from dim_location")
    print(result)
