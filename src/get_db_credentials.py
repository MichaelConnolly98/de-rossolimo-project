import boto3
import json
from botocore.exceptions import ClientError


def get_db_credentials(secret_name='totesys', sm_client=boto3.client('secretsmanager')):

    try:
        get_secret_value_response = sm_client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e

    secret = json.loads(get_secret_value_response['SecretString'])
    
    return secret