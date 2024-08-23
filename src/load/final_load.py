from utils.extract_data import get_connection
def lambda_handler(event, context):
    conn = get_connection(secret_name='totesys_data_warehouse')
    #this is final load