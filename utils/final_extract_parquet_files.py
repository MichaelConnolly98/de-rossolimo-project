import boto3
import os
from io import BytesIO
import pyarrow.parquet as pq
from botocore.exceptions import ClientError
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

if os.getenv("S3_PROCESS_BUCKET_NAME") is None:
    with open("s3_process_bucket_name.txt") as f:
        S3_BUCKET_NAME = f.readline()
else:
    S3_BUCKET_NAME = os.environ["S3_PROCESS_BUCKET_NAME"]


def extract_from_parquet(table_name, bucket_name=S3_BUCKET_NAME):
    try:
        # gets last modified file from table name provided
        get_last_modified = lambda obj: int(obj['LastModified'].strftime('%s'))
        s3_client = boto3.client("s3")
        response = s3_client.list_objects_v2(
            Bucket=bucket_name, Prefix=f"table={table_name}/")["Contents"]
        last_added = [obj['Key'] for obj in sorted(
            response, key=get_last_modified)][-1]

        # gets file contents from last modified file
        f = s3_client.get_object(Bucket=bucket_name, Key=last_added)
        buffer = BytesIO(f['Body'].read())
        # convert to table
        table = pq.read_table(buffer)
        # convert to dataframe
        df = table.to_pandas()
    except ClientError as c:
        logger.error(
            {"Result": "Failure", "Error": f"error occurred: {c.response}"}
            )
        raise c
    return df
