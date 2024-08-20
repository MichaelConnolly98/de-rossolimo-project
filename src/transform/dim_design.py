import pandas as pd
from src.transform.pandas_testing import dataframe_creator
import json


def design_dim():
    design_df = dataframe_creator(table_name="design")
    desired_columns_and_order = [
        "design_name", "file_location", "file_name"
        ]
    design_df = design_df[desired_columns_and_order]
    return design_df
