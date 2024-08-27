from utils.load_processed import load_processer
from utils.transformer import lambda_transformer
import logging
from botocore.exceptions import ClientError
import os
import json


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# I believe this is working; can upload first dump,
# then set it going with the eventbridge
if os.getenv("S3_PROCESS_BUCKET_NAME") is None:
    with open("s3_process_bucket_name.txt") as f:
        S3_BUCKET_NAME = f.readline()
else:
    S3_BUCKET_NAME = os.environ["S3_PROCESS_BUCKET_NAME"]


def lambda_handler(event, context):
    dataframe_dict = lambda_transformer()
    dataframe_successes = []
    for key, dataframe in dataframe_dict.items():
        dataframe_successes.append(
            load_processer(key, dataframe, bucket_name=S3_BUCKET_NAME))
    dataframe_successes = [x for x in dataframe_successes if x is not None]
    logger.info({"Result": "Success",
                 "Message": "Lambda Handler ran successfully"})
    return json.dumps(dataframe_successes)
