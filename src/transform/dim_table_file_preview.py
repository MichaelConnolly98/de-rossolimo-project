from pandas_testing import dataframe_creator
import pandas as pd
import iso4217parse
#might be that you want to be more specific - use drop method to remove certain columns by name 
#not by index reference
#use the id column from the table it's drawing from 
#need to change the indexes


def staff_dim():
    part_staff_df = dataframe_creator(table_name="staff")
    department_df = dataframe_creator(table_name="department")
    full_staff_df = pd.merge(part_staff_df, department_df[["department_id", "department_name", "location"]], on="department_id", how="left")
    #reorder columns
    cols = full_staff_df.columns.tolist()
    cols = cols[0:3] + cols[7:] + cols[4:5]
    full_staff_df = full_staff_df[cols]
    return full_staff_df

print(staff_dim())

def payment_dim():
    payment_df = dataframe_creator(table_name="payment_type")
    payment_df = payment_df.iloc[:,0:2]
    return payment_df

#small change
def counterparty_dim():
    part_counterparty_df = dataframe_creator(table_name="counterparty")
    address_df = dataframe_creator(table_name="address")
    full_counterparty_df = pd.merge(part_counterparty_df, address_df, left_on="legal_address_id",right_on="address_id", how="left")
    cols = full_counterparty_df.columns.tolist()
    cols = cols[0:2] + cols[8:15]
    full_counterparty_df = full_counterparty_df[cols]
    #fair few nones for address lines here, probably fine
    return counterparty_dim

def currency_name_maker(currency):
    """
    Takes a currency code and converts to a currency name
    """
    return iso4217parse.parse(currency)[0].name

def currency_dim():
    currency_df = dataframe_creator(table_name="currency")
    currency_df = currency_df.iloc[:, 0:2]
    currency_df["currency_name"] = currency_df["currency_code"].apply(currency_name_maker)
    return currency_df