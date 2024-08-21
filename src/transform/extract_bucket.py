from src.transform.dim_generator import create_date_table, currency_dim,\
payment_type_dim, staff_dim, counterparty_dim
from src.transform.fact_generator import sales_facts, payment_facts,\
purchase_order_facts
from src.transform.most_recent_pandas import file_data_single


#I believe this is working; can upload first dump, 
# then set it going with the eventbridge

def lambda_handler(event, context):
    file_dict = file_data_single()
    #date table only needs to be their for first upload
    dataframe_dict ={
        "date_table" : create_date_table(),
        "currency_table" : currency_dim(file_dict=file_dict),
        "payment_dim" : payment_type_dim(file_dict=file_dict),
        "staff_dim" : staff_dim(file_dict=file_dict),
        "counterparty_dim" : counterparty_dim(file_dict=file_dict),
        "sales_facts" : sales_facts(file_dict=file_dict),
        "payment_facts" : payment_facts(file_dict=file_dict),
        "purchase_order_facts" : purchase_order_facts(file_dict=file_dict)
    }

    # facts table generator from the most recent files
    # wouldn't upload dim tables if they haven't changed
    #how do we check if dim tables have changed?

    return dataframe_dict




