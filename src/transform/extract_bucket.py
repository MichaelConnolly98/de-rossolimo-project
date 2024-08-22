from dim_generator import create_date_table, currency_dim,\
payment_type_dim, staff_dim, counterparty_dim, location_dim, design_dim
from fact_generator import sales_facts, payment_facts,\
purchase_order_facts
from load_processed import load_processer
from most_recent_pandas import file_data_single
import logging
from botocore.exceptions import ClientError
import os


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

#I believe this is working; can upload first dump, 
# then set it going with the eventbridge
if os.getenv("S3_PROCESS_BUCKET_NAME") == None:
    with open("s3_process_bucket_name.txt") as f:
        S3_BUCKET_NAME = f.readline()
else:
    S3_BUCKET_NAME = os.environ["S3_PROCESS_BUCKET_NAME"]

def lambda_handler(event, context):
    dataframe_dict = lambda_transformer()
    for dataframe in dataframe_dict.values():
        load_processer(dataframe, bucket_name=S3_BUCKET_NAME)
    logger.info({{"Result": "Success", "Message": "Lambda Handler ran successfully"}})

def lambda_transformer():
    try:
        file_dict = file_data_single()
        #date table only needs to be their for first upload
        dataframe_dict ={
            "date_table" : create_date_table(),
            "currency_table" : currency_dim(file_dict=file_dict),
            "payment_dim" : payment_type_dim(file_dict=file_dict),
            "staff_dim" : staff_dim(file_dict=file_dict),
            "counterparty_dim" : counterparty_dim(file_dict=file_dict),
            "location_dim" :location_dim(file_dict=file_dict),
            "design_dim" : design_dim(file_dict=file_dict),
            "sales_facts" : sales_facts(file_dict=file_dict),
            "payment_facts" : payment_facts(file_dict=file_dict),
            "purchase_order_facts" : purchase_order_facts(file_dict=file_dict)
    
        }
        
        dataframe_values = [x for x in dataframe_dict.values() if x is None]
        if len(dataframe_values) == len(dataframe_dict):
            logging.warning("No dataframes created, all files empty")
        logging.info({"Result": "Success", "Message": "Lambda Transformer ran successfully"})
        return dataframe_dict
    
    except ClientError as c:
        logging.error(
            {"Result": "Failure", "Error": f"A Client Error error has occured: {str(e)}"}
        )
        raise c
    except KeyError as k:
        logging.error(
            {"Result": "Error", "Message": f"Missing required keys: {repr(k)}"}
        )
        raise k
    except Exception as e:
        logging.error(
            {"Result" : "Failure", "Error": f"Exception occured: {str(e)}"}
            )
        raise e
    




