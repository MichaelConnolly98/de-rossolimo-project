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
from sqlalchemy.types import Integer, String
from utils.load_connection_dbapi import connection


def load_dim_m(table_name, df, engine):
    result = df.to_sql(table_name, if_exists='append', con=engine, index=True)


def load_facts_m(table_name, df, engine):
    result = df.to_sql(table_name, if_exists='append', con=engine, index=False)


def load_transaction(df, conn):
    # Establish the connection to PostgreSQL using pg8000
    transaction_df = df
    cursor = conn.cursor()

    # Prepare the SQL INSERT statement template
    insert_query = '''
        INSERT INTO dim_transaction (transaction_id, transaction_type,
        sales_order_id, purchase_order_id)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (transaction_id)
        DO UPDATE SET
            transaction_type = EXCLUDED.transaction_type,
            sales_order_id = EXCLUDED.sales_order_id,
            purchase_order_id = EXCLUDED.purchase_order_id
        '''

    # Iterate through the DataFrame rows and insert each row into the table
    for index, row in transaction_df.iterrows():
        cursor.execute(insert_query, (
            row['transaction_id'],
            row['transaction_type'],
            row['sales_order_id'],
            row['purchase_order_id']
        ))

    # Commit the transaction
    conn.commit()
