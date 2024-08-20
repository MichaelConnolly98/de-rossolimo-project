from src.transform.pandas_testing import dataframe_creator
import pandas as pd

def staff_dim():
    part_staff_df = dataframe_creator(table_name="staff")
    department_df = dataframe_creator(table_name="department")

    full_staff_df = part_staff_df.join(
        department_df, on="department_id", how="left", rsuffix="a"
        )
    desired_columns_and_order = [
        "first_name", "last_name", "department_name", "location", "email_address"
        ]
    full_staff_df = full_staff_df[desired_columns_and_order]
    return full_staff_df

