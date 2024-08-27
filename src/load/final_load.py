from utils.extract_data import get_connection
def lambda_handler(event, context):
    conn = get_connection(secret_name='totesys_data_warehouse')
    #this is final load

    #event will have the table name
    #then parquet function takes the table name
    #and then the load dims file loads the table name
    #but conditional logic; if it's dim it's one, facts another, transaction another
    