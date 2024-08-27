import logging
from utils.dim_generator import currency_dim, payment_type_dim, staff_dim,\
location_dim, counterparty_dim, design_dim, create_date_table, transaction_dim
from utils.most_recent_pandas import file_data_single
from utils.fact_generator import sales_facts, payment_facts, purchase_order_facts
from botocore.exceptions import ClientError

def lambda_transformer():
    try:
        file_dict = file_data_single()
        #date table only needs to be their for first upload
        dataframe_dict ={
            "dim_currency" : currency_dim(file_dict=file_dict),
            "dim_payment_type" : payment_type_dim(file_dict=file_dict),
            "dim_staff" : staff_dim(file_dict=file_dict),
            "dim_counterparty" : counterparty_dim(file_dict=file_dict),
            "dim_location" :location_dim(file_dict=file_dict),
            "dim_design" : design_dim(file_dict=file_dict),
            "dim_transaction" : transaction_dim(file_dict=file_dict),
            "fact_sales_order" : sales_facts(file_dict=file_dict),
            "fact_payment" : payment_facts(file_dict=file_dict),
            "fact_purchase_order" : purchase_order_facts(file_dict=file_dict)
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
    