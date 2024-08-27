import pg8000
from pg8000.native import identifier
from utils.extract_data import get_connection
from utils.extract_data import get_db_credentials
from pprint import pprint
import pandas as pd


def load_dims(table_name, df):
    with get_connection('totesys_data_warehouse') as conn:
        for index, row in df.iterrows():
            columns = str(df.index.name) + ', ' + ', '.join(row.keys())
            values = "'" + str(index) + "', '" + "', '".join(
                map(str, row.values)) + "'"
            sql_insert = f"""INSERT INTO {identifier(table_name)}
                        ({columns})
                        VALUES ({values});"""
            response = conn.run(sql_insert)
    return response
