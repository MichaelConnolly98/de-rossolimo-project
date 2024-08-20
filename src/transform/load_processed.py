"placeholder so not empty"
import pandas as pd
import boto3
from io import BytesIO


# takes dataframe, puts it into parquet, stores as buffer, then saves buffer to s3 bucket
# buffer stops need to save locally problems might arise if data is longer than memory
# - look out for this !!

def load_processed(df):
    #aws client that connects to s3
    s3 = boto3.client('s3')
    #temperary place to store bytes of parquet
    out_buffer = BytesIO()
    # put parquet into temp store
    df.to_parquet(out_buffer)
    #upload to s3 bucketname is placeholder for now
    s3.put_object(
        Bucket = 'BUCKETNAME',
        Key = 'test',
        Body = out_buffer.getvalue()
    )
    # return value is logged?
    