import json
import boto3
import os
from datetime import datetime
import logging
from botocore.exceptions import ClientError

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
        for key, value in data['all_data'].items():
            s3.put_object(
                Bucket = (BUCKETNAME),
                Key = (f'table={key}/year={date.year}/month={date.month}/day={date.day}/{folder_name_2}.json'),
                Body = json.dumps({key: value}, default=str)
            )

        logger.info(f'success at {folder_name} {folder_name_2}')
        return {'result': 'success'}
    except TypeError as t:
        logger.error(f'error occurred: {repr(t)}')
        return t
            
    except ClientError as c:
        logger.error(f'error occurred: {c.response}')
        return c
    
    except Exception as e:
        logger.error(f'error occurred while trying to upload to s3 bucket: {repr(e)}')
        return e
    
    