from utils.final_load_pq_to_df import load_pq_to_df
from utils.pandas_testing import dataframe_creator
from utils.load_processed import load_processer
import pandas as pd
from moto import mock_aws
import boto3
import pytest
import os
from io import BytesIO
import pyarrow.parquet as pq
import logging
from unittest.mock import patch
from datetime import datetime
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

# @patch('src.transform.load_processed.datetime')
# def test_func_takes_pq_file(s3_client, datetime_patch):
#     datetime_patch.now.return_value = datetime(2002, 11, 9, 16, 38, 23)
#     dataf = dataframe_creator('address', file_dict)
#     result = load_processer(dataf, "test-bucket")
    
#     s3_object = s3_client.get_object(Bucket='test-bucket', Key='table=address/year=2002/month=11/day=9/16:38:23.parquet')
    
#     buffer = BytesIO(s3_object['Body'].read())

#     load_to_dw(buffer)