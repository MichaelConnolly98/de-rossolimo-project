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
        INSERT INTO dim_transaction (transaction_id, transaction_type, sales_order_id, purchase_order_id)
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

def load_counterparty(df, conn):
    counterparty_df = df
    cursor = conn.cursor()
    insert_query = '''
        INSERT INTO dim_counterparty (counterparty_id, counterparty_legal_name, counterparty_legal_address_line_1, counterparty_legal_address_line_2, counterparty_legal_district, counterparty_legal_city, counterparty_legal_postal_code, counterparty_legal_country, counterparty_legal_phone_number)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (counterparty_id)
        DO UPDATE SET
            counterparty_legal_name  = EXCLUDED.counterparty_legal_name,
            counterparty_legal_address_line_1 = EXCLUDED.counterparty_legal_address_line_1,
            counterparty_legal_address_line_2 = EXCLUDED.counterparty_legal_address_line_2,
            counterparty_legal_district = EXCLUDED.counterparty_legal_district,
            counterparty_legal_city  = EXCLUDED.counterparty_legal_city,
            counterparty_legal_postal_code = EXCLUDED.counterparty_legal_postal_code,
            counterparty_legal_country = EXCLUDED.counterparty_legal_country,
            counterparty_legal_phone_number = EXCLUDED.counterparty_legal_phone_number
        '''
    
    for index, row in counterparty_df.iterrows():
        cursor.execute(insert_query, (
            row['counterparty_id'],
            row['counterparty_legal_name'], 
            row['counterparty_legal_address_line_1'], 
            row['counterparty_legal_address_line_2'],  
            row['counterparty_legal_district'],
            row['counterparty_legal_city'],
            row['counterparty_legal_postal_code'],
            row['counterparty_legal_country'],
            row['counterparty_legal_phone_number']
        ))

    # Commit the transaction
    conn.commit()
    
def load_design(df, conn):
    # Establish the connection to PostgreSQL using pg8000
    design_df = df
    cursor = conn.cursor()

    # Prepare the SQL INSERT statement template
    insert_query = '''
        INSERT INTO dim_design (design_id, design_name, file_location, file_name )
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (design_id)
        DO UPDATE SET
            design_name = EXCLUDED.design_name,
            file_location = EXCLUDED.file_location,
            file_name = EXCLUDED.file_name
        '''
    
    # Iterate through the DataFrame rows and insert each row into the table
    for index, row in design_df.iterrows():
        cursor.execute(insert_query, (
            row['design_id'], 
            row['design_name'], 
            row['file_location'],  
            row['file_name']  
        ))

    # Commit the transaction
    conn.commit()


def load_location(df, conn):
    location_df = df
    cursor = conn.cursor()

    # Prepare the SQL INSERT statement template
    insert_query = '''
        INSERT INTO dim_location (location_id, address_line_1, address_line_2, district, city, postal_code, country, phone)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (location_id)
        DO UPDATE SET
            address_line_1 = EXCLUDED.address_line_1,
            address_line_2 = EXCLUDED.address_line_2,
            district = EXCLUDED.district,
            city = EXCLUDED.city,
            postal_code = EXCLUDED.postal_code,
            country = EXCLUDED.country,
            phone = EXCLUDED.phone
        '''
    
    # Iterate through the DataFrame rows and insert each row into the table
    for index, row in location_df.iterrows():
        cursor.execute(insert_query, (
            row['location_id'],  
            row['address_line_1'], 
            row['address_line_2'], 
            row['district'],  
            row['city'], 
            row['postal_code'], 
            row['country'],
            row['phone'] 
        ))

            # Commit the transaction
    conn.commit()





