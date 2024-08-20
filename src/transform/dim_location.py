import pandas as pd
from src.transform.pandas_testing import dataframe_creator
import json

"""
Location dimensions table - not sure what columns should be in here, thought to add staff names 
but then thought that would be a repitition of staff_dims essentially

"""

def location_dim():
    location_df = dataframe_creator(table_name="department")
    desired_columns_and_order = [
    "location", "department_name"
    ]
    location_df = location_df[desired_columns_and_order]
    return location_df



