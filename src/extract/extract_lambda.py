from extract import get_db_credentials, get_connection, extract
from load import load
import os
import pprint


def lambda_handler(event, context):

    data = extract()
    result = load(data)


lambda_handler(0, 0)
