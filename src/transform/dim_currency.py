import boto3
import os
from pprint import pprint
import json
import pandas as pd
from src.transform.currency_code_to_name import currency_code_to_name

def currency_dim(currency_df):
    currency_df = currency_df.drop(['created_at', 'last_updated'], axis=1)
    currency_df["currency_name"] = currency_df["currency_code"].apply(currency_code_to_name)
    return currency_df