from utils.extract_data import get_db_credentials
from utils.final_extract_parquet_files import extract_from_parquet
from utils.final_load_dims_m import load_facts_m, load_transaction
import os
from utils.load_non_local_connect import non_local_db_connect
import sqlalchemy
import logging
from pg8000 import DatabaseError




if os.getenv("S3_PROCESS_BUCKET_NAME") == None:
    with open("s3_process_bucket_name.txt") as f:
        S3_BUCKET_NAME = f.readline()
else:
    S3_BUCKET_NAME = os.environ["S3_PROCESS_BUCKET_NAME"]

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


secret = get_db_credentials("totesys_data_warehouse")
engine = sqlalchemy.create_engine(f'postgresql+pg8000://{secret["username"]}:{secret["password"]}@{secret["host"]}:{secret["port"]}/{secret["dbname"]}')
conn_dbapi = non_local_db_connect(secret)



def lambda_handler(event, context):

   
    conn_dbapi = non_local_db_connect(secret)
    try:
        for table_name in event:
            print(table_name)
            dataframe = extract_from_parquet(table_name, S3_BUCKET_NAME)
            if table_name in ["dim_transaction"]:
                load_transaction(dataframe, conn_dbapi)
            elif table_name in ["fact_sales_order", "fact_payment", "fact_purchase_order"]:
                load_facts_m(table_name, dataframe, engine)
        logging.info({"Result": "Success", "Message": "Database updated"})
    except DatabaseError as d:
        logging.error({"Result": "Failure", "error": f"Database Error occured: {d}"})
        raise d
    



