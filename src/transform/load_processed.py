"placeholder so not empty"
import pandas as pd
import boto3
from io import BytesIO
import logging
from datetime import datetime

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# takes dataframe, puts it into parquet, stores as buffer, then saves buffer to s3 bucket
# buffer stops need to save locally problems might arise if data is longer than memory
# - look out for this !!

def load_processed(df):
    try:
        table_name = df.name
        date = datetime.now()
        folder_name = datetime.now().strftime("%Y-%m-%d")
        folder_name_2 = datetime.now().strftime("%H:%M:%S")
        #aws client that connects to s3
        s3 = boto3.client('s3')
        #temperary place to store bytes of parquet
        out_buffer = BytesIO()  
        # put parquet into temp store
        df.to_parquet(out_buffer)
        #upload to s3 bucketname is placeholder for now
        s3.put_object(
            Bucket = 'test-bucket',
            Key = f"table={table_name}/year={date.year}/month={date.month}/day={date.day}/{folder_name_2}.parquet",
            Body = out_buffer.getvalue()
        )
        # return value is logged?
    except AttributeError as ae:
        logger.error({"Result": "Failure", "Error": f"AttributeError occurred: {str(ae)}"})
        raise ae 