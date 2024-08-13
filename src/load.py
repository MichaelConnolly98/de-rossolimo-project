import json
import boto3
import os
from datetime import datetime
import logging

logger = logging.getLogger("MyLogger")
logger.setLevel(logging.INFO)

""" writes data to date encoded s3 folder split into table folders """


def load(data):

    s3 = boto3.client('s3')
    BUCKETNAME = os.environ['S3_BUCKET_NAME']
    date = datetime.now()
    folder_name = datetime.now().strftime("%Y-%m-%d")
    folder_name_2 = datetime.now().strftime('%H:%M:%S')

    try: 
        s3.put_object(
            Bucket = (BUCKETNAME),
            Key = (f'year={date.year}/month={date.month}/day={date.day}/{folder_name_2}'),
            Body = json.dumps(data)
        )
        logger.info(f'success at {folder_name} {folder_name_2}')
        return {'result': 'success'}
    except Exception as e:
        logger.error(f'error occurred while trying to upload to s3 bucket')
        return e
        