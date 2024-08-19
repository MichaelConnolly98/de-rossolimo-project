import json
import boto3
import os
from datetime import datetime
import logging
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class BodyNotUploadedError:
    pass

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

    try:
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
            logger.warning("All tables will be uploaded as empty files")


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
                    {"Result": "Failure",\
                    "Error": f" body not uploaded at {folder_name} {folder_name_2}"}
                )
    
                raise BodyNotUploadedError(
                    f"error at {folder_name} {folder_name_2}")

        logger.info({"Result": "Success", "Message": f"data uploaded at {folder_name} {folder_name_2}"})
        return {"Result": "Success", "Message": "data uploaded"}

    except TypeError as t:
        logger.error({"Result": "Failure", "Error": f"TypeError occurred: {str(t)}"})
        raise TypeError('A TypeError has occured')

    except ClientError as c:
        logger.error(
            {"Result": "Failure", "Error": f"error occurred: {c.response}"}
            )
        raise c

    except Exception as e:
        logger.error({"Result": "Failure",\
                    "Error": f"Exception occurred on upload to s3 bucket: {str(e)}"})
        raise e
