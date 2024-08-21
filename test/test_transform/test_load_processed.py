from src.transform.load_processed import load_processed
from src.transform.pandas_testing import dataframe_creator
import pandas as pd
from moto import mock_aws
import boto3
import pytest
import os
from io import BytesIO
import pyarrow.parquet as pq
import logging
import json


logger = logging.getLogger("test")
logger.setLevel(logging.INFO)
logger.propagate = True

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

with open("pandas_test_data_copy.json", "r") as f:
    file_dict=json.load(f)

def test_func_transforms_to_parquet(s3_client):
    dataf = dataframe_creator('address', file_dict=file_dict)
    load_processed(dataf)
    
    #get object just loaded in
    s3_object = s3_client.get_object(Bucket='test-bucket', Key='test')
    #read bytes
    buffer = BytesIO(s3_object['Body'].read())
    #convert to table
    table = pq.read_table(buffer)
    #convert to dataframe
    df = table.to_pandas()
    #assert dataframe from s3 bucket is same as dataframe passed to function
    assert df.equals(dataf)

def test_raises_exception_when_not_passed_df(s3_client, caplog):
    fake_data = {
        "all_data": {
            "table1": [
                {"house_number": 5, "street": "first_street"},
                {"house_number": 6, "street": "second_street"},
            ],
            "table2": [
                {"seller_id": 1, "name": "Nick"},
                {"seller_id": 2, "name": "Mike"},
            ],
        }
    }
    with caplog.at_level(logging.INFO):
        with pytest.raises(AttributeError):
            load_processed(fake_data)
            assert {'Result': "Failure", 'Error': "AttributeError occurred:"} in caplog.text
