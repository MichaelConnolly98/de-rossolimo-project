import pandas as pd
from src.transform.pandas_testing import dataframe_creator

def counterparty_dim():
    part_counterparty_df = dataframe_creator(table_name="counterparty")
    address_df = dataframe_creator(table_name="address")
    address_df["legal_address_id"] = address_df.index
    

    full_counterparty_df = part_counterparty_df.join(
        address_df, on="legal_address_id", how="left", rsuffix="a")
    desired_columns_and_order = [
        "counterparty_legal_name", "address_line_1", "address_line_2", "district", "city", "postal_code", "country", "phone"
    ]
    
    
    full_counterparty_df = full_counterparty_df[desired_columns_and_order]
    full_counterparty_df.rename(columns={
        "address_line_1" : "counterparty_legal_address_line_1",
        "address_line_2" : "counterparty_legal_address_line_2",
        "district" : "counterparty_legal_district",
        "city" : "counterparty_legal_city",
        "postal_code" : "counterparty_legal_postal_code",
        "country": "counterparty_legal_country",
        "phone" : "counterparty_legal_phone_number"
    }, inplace=True)
    return full_counterparty_df



