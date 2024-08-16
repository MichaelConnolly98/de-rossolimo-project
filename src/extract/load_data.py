import json
import boto3
import os
from datetime import datetime
import logging
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def load(data):
    """
    Writes database data to individual table folders in s3 bucket

    Data will be written to buckets in filepaths of format:
    table={table_name}/year={year}/month={month}/day={day}/time.json

    Parameters:
    data - dictionary of format
    {'all_data': {'table_1: data}, {'table_2: data}, ...}

    Returns:
    None
    """
    s3 = boto3.client("s3")
    BUCKETNAME = os.environ["S3_BUCKET_NAME"]
    date = datetime.now()
    folder_name = datetime.now().strftime("%Y-%m-%d")
    folder_name_2 = datetime.now().strftime("%H:%M:%S")
    counter = 0

    for table in data["all_data"]:
        if not data["all_data"][table]:
            counter += 1

    if counter == len(data["all_data"]):
        logger.warning("All tables uploaded as empty files")

    try:
        for key, value in data["all_data"].items():
            s3.put_object(
                Bucket=(BUCKETNAME),
                Key=(
                    f"table={key}/year={date.year}/month={date.month}/day={date.day}/{folder_name_2}.json"
                ),
                Body=json.dumps({key: value}, default=str),
            )
            response = s3.get_object(
                Bucket=(BUCKETNAME),
                Key=(
                    f"table={key}/year={date.year}/month={date.month}/day={date.day}/{folder_name_2}.json"
                ),
            )
            response_body = response["Body"].read().decode("utf-8")
            if response_body is None:
                logger.error(
                    f"error occurred:"
                    f" body not uploaded at {folder_name} {folder_name_2}"
                )
                return f"error at {folder_name} {folder_name_2}"

        logger.info(f"success at {folder_name} {folder_name_2}")
        return {"result": "success"}

    except TypeError as t:
        logger.error(f"error occurred: {repr(t)}")
        return t

    except ClientError as c:
        logger.error(f"error occurred: {c.response}")
        return c

    except Exception as e:
        logger.error(
            f"error occurred while trying to upload to s3 bucket: {repr(e)}"
            )
        return e
