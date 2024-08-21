from src.transform.dim_generator import create_date_table, currency_dim,\
payment_type_dim, staff_dim, counterparty_dim
from src.transform.read_ingestion import read_ingestion_function

def lambda_handler(event, context):
    dataframes = read_ingestion_function()
    dataframe_dim_dict ={
        "date_table" : create_date_table(),
        "currency_table" : currency_dim(dataframes["currency"]),
        "payment_dim" : payment_type_dim(dataframes["payment_type"]),
        "staff_dim" : staff_dim(),
        "counterparty_dim" : counterparty_dim()
    }

    # facts table generator from the most recent files
    # wouldn't upload dim tables if they haven't changed
    #how do we check if dim tables have changed?





    return dataframe_dim_dict



