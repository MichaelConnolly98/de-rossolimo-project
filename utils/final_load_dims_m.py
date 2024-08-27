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
from test_warehouse_data.connection_dbapi import connection
            
def load_dim_m(table_name, df, engine):
    result = df.to_sql(table_name, if_exists='append', con=engine, index=True)

def load_facts_m(table_name, df, engine):
    result = df.to_sql(table_name, if_exists='append', con=engine, index=False)

# def load_transaction_dim(table_name, df, engine):
#     result = df.to_sql(table_name, if_exists='append', con=engine, index=True, dtype={
#         "transaction_id" : Integer(),
#         "transaction_type" : String(),
#         "sales_order_id" : Integer(),
#         "purchase_order_id" : Integer()
#     })

#find a way to apply this to all dim tables below

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
    

    # except Exception as e:
    #     print(f"An error occurred: {e}")
    #     conn.rollback()
    # # finally:
    #     cursor.close()
    #     conn.close()

# def load_payment(payment_fact_df, conn):
#     insert_query = '''
#         INSERT INTO fact_payment ( payment_id, created_date, created_time, last_updated_date, last_updated_time, transaction_id, counterparty_id, payment_amount, currency_id, payment_type_id, paid, payment_date) 
#         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
#     cursor = conn.cursor()
#     for index, row in payment_fact_df.iterrows():
#         cursor.execute(insert_query, (
#             row['payment_id'], 
#             row['created_date'], 
#             row['created_time'],  
#             row['last_updated_date'],
#             row['last_updated_time'],
#             row['transaction_id'],
#             row['counterparty_id'],
#             row['payment_amount'],
#             row['currency_id'],
#             row['payment_type_id'],
#             row['paid'],
#             row['payment_date']
#         ))
#     conn.commit()

# def load_purchase_order(purchase_order_df, conn):
#     insert_query = '''
#     INSERT INTO fact_purchase_order ( payment_id, created_date, created_time, last_updated_date, last_updated_time, transaction_id, counterparty_id, payment_amount, currency_id, payment_type_id, paid, payment_date) 
#     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
#     cursor = conn.cursor()
#     for index, row in purchase_order_df.iterrows():
#         cursor.execute(insert_query, (
#             row['purchase_order_id'], 
#             row['created_date'], 
#             row['created_time'],  
#             row['last_updated_date'],
#             row['last_updated_time'],
#             row['staff_id'],
#             row['counterparty_id'],
#             row['purchase_record_id'],
#             row['item_quantity'],
#             row['item_unit_price'],
#             row['currency_id'],
#             row['agreed_delivery_date'],
#             row['agreed_payment_date'],
#             row['item_code'],
#             row['agreed_delivery_location_id']
#         ))

