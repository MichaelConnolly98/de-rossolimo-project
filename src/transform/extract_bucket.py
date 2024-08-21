from src.transform.dim_generator import create_date_table, currency_dim,\
payment_type_dim, staff_dim, counterparty_dim
from src.transform.pandas_testing import file_data


def lambda_handler(event, context):
    file_dict = file_data()

    dataframe_dim_dict ={
        "date_table" : create_date_table(),
        "currency_table" : currency_dim(file_dict=file_dict),
        "payment_dim" : payment_type_dim(file_dict=file_dict),
        "staff_dim" : staff_dim(file_dict=file_dict),
        "counterparty_dim" : counterparty_dim(file_dict=file_dict)
    }

    # facts table generator from the most recent files
    # wouldn't upload dim tables if they haven't changed
    #how do we check if dim tables have changed?

    return dataframe_dim_dict

print(lambda_handler(0,0))



