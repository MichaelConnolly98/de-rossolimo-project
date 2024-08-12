
from pg8000.native import Connection
import boto3
import json
from botocore.exceptions import ClientError

"""extracts data from database"""

def lambda_handler(event, context):
    pass


def get_db_credentials(secret_name='totesys', sm_client=boto3.client('secretsmanager')):

    try:
        get_secret_value_response = sm_client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e

    secret = json.loads(get_secret_value_response['SecretString'])
    
    return secret

def get_connection():
    credentials_dict = get_db_credentials()
    return Connection(
        user=credentials_dict["username"],
        password=credentials_dict["password"],
        database=credentials_dict["database"],
        host=credentials_dict["host"],
        port=credentials_dict["port"]
        )


