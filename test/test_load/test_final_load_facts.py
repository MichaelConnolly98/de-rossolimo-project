from utils.final_load_facts import load_facts
from utils.fact_generator import sales_facts
import json
import pytest

with open("pandas_test_data_copy.json", "r") as f:
    file_dict=json.load(f)

test_df = sales_facts(file_dict=file_dict)

def test_load_facts_xxcccfsfsgx():
    load_facts('fact_sales_order', test_df)
    assert False