from extract_data import extract
from load_data import load
from log_time import get_timestamp_from_logs
import os
import pprint
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
def lambda_handler(event, context):
    try:
        data = extract(get_timestamp_from_logs())
        result = load(data)
        print(result)
    except Exception as e:
        logger.info(f"Unexpected Exception: {str(e)}")


