"placeholder so not empty"
import pandas as pd
import boto3


# takes dataframe, puts it into parquet, stores as buffer, then saves buffer to s3 bucket
# buffer stops need to save locally

def load_processed(df):
    s3 = boto3.client('s3')
    # s3.put_object
    # df.to_parquet()
    # return pq