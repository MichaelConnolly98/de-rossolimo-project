from src.transform.load_processed import load_processed
from src.transform.pandas_testing import dataframe_creator
import json
import pandas as pd

def test_func_transforms_to_parquet():
    dataf = dataframe_creator('address')
    print(load_processed(dataf))