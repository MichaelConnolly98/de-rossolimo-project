
from pg8000.native import Connection, literal, identifier
import boto3
import json
from botocore.exceptions import ClientError
from pprint import pprint
import json


"""
Extract from database

Convert to JSON

Confirm how the data is passed to Michael
"""



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
        database=credentials_dict["dbname"],
        host=credentials_dict["host"],
        port=credentials_dict["port"]
        )


def extract():
    with get_connection() as conn:

        table_names_sql_query = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema='public'
        AND table_type='BASE TABLE'
        """

        table_names_nested_list = conn.run(table_names_sql_query)
        table_names_flattened_list = [element[0] for element in table_names_nested_list if element[0] != '_prisma_migrations']

    # print (table_names_flattened_list)

    def query_table(table_name):
        with get_connection() as conn:
            table_query = f"""SELECT * FROM {identifier(table_name)};"""
            data = conn.run(table_query)
            columns = [column['name'] for column in conn.columns]

            results_list = []
            for row in data:
                result = dict(zip(columns, row))
                results_list.append(result)
            return results_list


    data_dict = {}
    for table in table_names_flattened_list:
        data_dict[table] = query_table(table)
    
    pprint (data_dict["address"])

    all_data_dict = {"all_data": data_dict}

    return all_data_dict
 


extract()

