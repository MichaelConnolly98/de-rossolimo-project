import pg8000
from pg8000.native import identifier
from utils.extract_data import get_connection
from utils.extract_data import get_db_credentials
from pprint import pprint
import pandas as pd
import sqlalchemy
from utils.dim_generator import location_dim
import json

with open("pandas_test_data_copy.json", "r") as f:
    file_dict=json.load(f)
            
def load_dim_m(table_name, df, engine):
    result = df.to_sql(table_name, if_exists='append', con=engine)
    



#load_dims("dim_location", test_df)