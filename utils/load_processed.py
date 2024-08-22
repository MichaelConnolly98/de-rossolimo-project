import pandas as pd
import boto3
from io import BytesIO
import logging
from datetime import datetime
import os

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# takes dataframe, puts it into parquet, stores as buffer, then saves buffer to s3 bucket
# buffer stops need to save locally problems might arise if data is longer than memory
# - look out for this !!

if os.getenv("S3_PROCESS_BUCKET_NAME") == None:
    with open("s3_process_bucket_name.txt") as f:
        S3_BUCKET_NAME = f.readline()
else:
    S3_BUCKET_NAME = os.environ["S3_PROCESS_BUCKET_NAME"]

def load_processer(df, bucket_name=S3_BUCKET_NAME):
    try:
        date = datetime.now()
        folder_name = datetime.now().strftime("%Y-%m-%d")
        folder_name_2 = datetime.now().strftime("%H:%M:%S")
        if df is not None:
            table_name = df.name
            
            #aws client that connects to s3
            s3 = boto3.client('s3')
            #temperary place to store bytes of parquet
            out_buffer = BytesIO()  
            # put parquet into temp store
            df.to_parquet(out_buffer)
            #upload to s3 bucketname is placeholder for now
            s3.put_object(
                Bucket = bucket_name,
                Key = f"table={table_name}/year={date.year}/month={date.month}/day={date.day}/{folder_name_2}.parquet",
                Body = out_buffer.getvalue()
            )
            # return value is logged?
            logger.info({"Result": "Success", "Message": f"{table_name} data uploaded at {folder_name} {folder_name_2}"})
            return {"Result": "Success", "Message": "data uploaded"}

        else:
            logger.info({"Message": f"no data to upload at {folder_name} {folder_name_2}"})
            return {"Message": "no data to upload"}
    except AttributeError as ae:
        logger.error({"Result": "Failure", "Error": f"AttributeError occurred: {str(ae)}"})
        raise ae 