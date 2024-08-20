from src.transform.load_processed import load_processed
from src.transform.pandas_testing import dataframe_creator
import pandas as pd
from moto import mock_aws
import boto3
import pytest
import os
from io import BytesIO, StringIO
import pyarrow.parquet as pq

@pytest.fixture()
def aws_creds():
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"
    os.environ["S3_BUCKET_NAME"] = "test-bucket"


@pytest.fixture()
def s3_client(aws_creds):
    with mock_aws():
        s3 = boto3.client("s3")
        s3.create_bucket(
            Bucket="test-bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        yield s3


# def test_func_transforms_to_parquet(s3_client):
#     dataf = dataframe_creator('address')
#     load_processed(dataf)

#     #get object just loaded in
#     s3_object = s3_client.get_object(Bucket='test-bucket', Key='test')
#     #get streaming body of object
#     sb = s3_object['Body']
#     #read streaming body
#     parq = sb.read()

#     in_buffer = BytesIO(parq)
#     df = pd.DataFrame(in_buffer)
#     print(df)
    
    # read into dataframe and compare