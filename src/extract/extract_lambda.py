from src.extract.extract_data import get_db_credentials, get_connection, extract
from src.extract.load_data import load
import os
import pprint


def lambda_handler(event, context):
    
    data = extract()
    result = load(data)

lambda_handler(0, 0)