from get_db_credentials import get_db_credentials as credentials_dict
from pg8000.native import Connection

def get_connection():
    return Connection(
        user=credentials_dict["username"],
        password=credentials_dict["password"],
        database=credentials_dict["database"],
        host=credentials_dict["host"],
        port=credentials_dict["port"]
        )


"""extracts data from database"""

def lambda_handler(event, context):
    pass