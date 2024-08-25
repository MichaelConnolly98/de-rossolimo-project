import pg8000
from pg8000.native import identifier
from utils.extract_data import get_connection
from utils.extract_data import get_db_credentials
from pprint import pprint
import pandas as pd
import sqlalchemy
from utils.dim_generator import location_dim
import json
from pg8000.exceptions import InterfaceError
            
def load_dim_m(table_name, df, engine):
    result = df.to_sql(table_name, if_exists='append', con=engine)

def load_facts_m(table_name, df, engine):
    result = df.to_sql(table_name, if_exists='append', con=engine, index=False, index_label="sales_record_id")


