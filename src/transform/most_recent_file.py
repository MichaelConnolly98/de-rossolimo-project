import boto3
from botocore.exceptions import ClientError
import logging
from pandas_testing import get_table_names
import os



if os.environ.get["S3_DATA_BUCKET_NAME"] != None:
    S3BUCKETDATA = os.environ["S3_DATA_BUCKET_NAME"]
else:
    S3BUCKETDATA = "de-rossolimo-ingestion-20240812125359611100000001"

def get_most_recent_key_per_table_from_s3(s3_table_name_prefix, bucket_name=S3BUCKETDATA):
    """
    Gets all key paths for a file prefix in s3 bucket

    Parameters:
    s3_table_name_prefix - string of a prefix for a filepath in s3
    bucket_name - string of a bucket name

    Returns:
    List of keys for a particular file prefix
    """
    get_last_modified = lambda obj: int(obj['LastModified'].strftime('%s'))

    try:
        s3_client = boto3.client("s3")
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=f"table={s3_table_name_prefix}/")["Contents"]
        last_added = [obj['Key'] for obj in sorted(response, key=get_last_modified)][-1]
        return last_added
    
    except ClientError as e:
        logging.error(
            {"Result": "Failure", "Error": f"A Client Error error has occured: {str(e)}"}
        )
        raise e
    except KeyError as k:
        if "Contents" in str(k):
            logging.error({"Result": "Failure", "Error": f"No contents in Prefix or Bucket, {str(k)}"})
        raise k

    # except Exception as exception:
    #     logging.error({"Result": "Failure", "Error": f"An exception has occured: {str(exception)}"})
    #     raise Exception("An error has occured")
    


# bucket_name = "actual_bucket_name"
# prefix = "path/to/files/"
    


# s3 = boto3.client('s3')
# objs = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter='/' ['Contents']
# last_added = [obj['Key'] for obj in sorted(objs, key=get_last_modified)][0]