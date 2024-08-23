from utils.final_load_dims import load_dims
from utils.dim_generator import location_dim
import json
import pytest

with open("pandas_test_data_copy.json", "r") as f:
    file_dict=json.load(f)

test_df = location_dim(file_dict=file_dict)

def test_load_facts_xxcccfsfsgx():
    load_dims('dim_location', test_df)
    assert False