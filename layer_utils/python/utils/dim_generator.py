import pandas as pd
from utils.currency_code_to_name import currency_code_to_name
from utils.most_recent_pandas import dataframe_creator_single
import json
import numpy as np

def create_date_table(
        start='2000-01-01',
          end='2050-12-31'
    ):
    """
    Creates a date dimension table that should not need to be modified
    after creation

    Should only be ran once to generate original table
    """
   
    df = pd.DataFrame(
        {"Date": pd.date_range(start, end)}
        )
    df["year"] = df.Date.dt.year
    df["month"] = df.Date.dt.month
    df["day"] = df.Date.dt.day
    df["day_of_week"] = df.Date.dt.day_of_week
    df["day_name"] = df.Date.dt.day_name()
    df["month_name"] = df.Date.dt.month_name()
    df["quarter"] = df.Date.dt.quarter
    df.set_index("Date", inplace=True)
    df.index.name="date_id"
    return df


def currency_dim(file_dict=None):
    currency_df = dataframe_creator_single("currency", file_dict)
    if isinstance(currency_df, pd.DataFrame):
        currency_df = currency_df.drop(['created_at', 'last_updated'], axis=1)
        currency_df["currency_name"] = currency_df["currency_code"].apply(currency_code_to_name)
        return currency_df
    else:
        return None

def payment_type_dim(file_dict=None):
    payment_df = dataframe_creator_single("payment_type", file_dict)
    if isinstance(payment_df, pd.DataFrame):
        payment_df = payment_df.drop(['created_at', 'last_updated'], axis=1)
        return payment_df
    else:
        return None

def staff_dim(file_dict=None):
    part_staff_df = dataframe_creator_single("staff", file_dict)
    if isinstance(part_staff_df, pd.DataFrame):
        department_df = dataframe_creator_single("department", file_dict)

        full_staff_df = part_staff_df.join(
            department_df, on="department_id", how="left", rsuffix="a"
            )
        desired_columns_and_order = [
            "first_name", "last_name", "department_name", "location", "email_address"
            ]
        full_staff_df = full_staff_df[desired_columns_and_order]
        return full_staff_df
    else:
        return None

def counterparty_dim(file_dict=None):
    part_counterparty_df = dataframe_creator_single("counterparty", file_dict)
    if isinstance(part_counterparty_df, pd.DataFrame):
        address_df = dataframe_creator_single("address", file_dict)
        address_df["legal_address_id"] = address_df.index

        
        
        full_counterparty_df = part_counterparty_df.join(
            address_df, on="legal_address_id", how="left", rsuffix="a")
        
        full_counterparty_df["counterparty_id_new"] = full_counterparty_df.index
        full_counterparty_df.index.name = "index"
        full_counterparty_df = full_counterparty_df.rename(columns={
            "counterparty_id_new" : "counterparty_id"
        })

        desired_columns_and_order = [
            "counterparty_id", "counterparty_legal_name", "address_line_1", "address_line_2", "district", "city", "postal_code", "country", "phone"
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
    else:
        return None


def location_dim(file_dict=None):
    location_df = dataframe_creator_single("address", file_dict)
    if isinstance(location_df, pd.DataFrame):
        location_df["location_id_new"] = location_df.index
        location_df.index.name = "index"
        location_df = location_df.rename(columns={
            "location_id_new" : "location_id"
        })
        location_df = location_df.drop(['created_at', 'last_updated'], axis=1)
        return location_df
    else:
        return None


def design_dim(file_dict=None):
    design_df = dataframe_creator_single("design", file_dict)
    if isinstance(design_df, pd.DataFrame):

        design_df["design_id_new"] = design_df.index
        design_df.index.name = "index"
        design_df = design_df.rename(columns={
            "design_id_new" : "design_id"
        })

        desired_columns_and_order = [
            "design_id", "design_name", "file_location", "file_name"
            ]
        design_df = design_df[desired_columns_and_order]
        return design_df
    else:
        return None
    
def transaction_dim(file_dict=None):
    transaction_df = dataframe_creator_single("transaction", file_dict)
    if isinstance(transaction_df, pd.DataFrame):
        transaction_df["transaction_id_new"] = transaction_df.index
        transaction_df.index.name = "index"
        transaction_df = transaction_df.rename(columns={
            "transaction_id_new" : "transaction_id"
        })

        transaction_df = transaction_df.drop(['created_at', 'last_updated'], axis=1)
        desired_cols_and_order = ["transaction_id", "transaction_type", "sales_order_id", "purchase_order_id"]
        transaction_df = transaction_df[desired_cols_and_order]
        transaction_df["sales_order_id"] = transaction_df["sales_order_id"].astype('Int64')
        transaction_df["sales_order_id"] = transaction_df["sales_order_id"].astype(object).where(pd.notnull(transaction_df["sales_order_id"]), None)
        transaction_df["purchase_order_id"] = transaction_df["purchase_order_id"].astype('Int64')
        transaction_df["purchase_order_id"] = transaction_df["purchase_order_id"].astype(object).where(pd.notnull(transaction_df["purchase_order_id"]), None)
        return transaction_df
    else:
        return None

