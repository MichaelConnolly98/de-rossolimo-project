from utils.final_extract_parquet_files import extract_from_parquet
from utils.pandas_testing import dataframe_creator
from utils.load_processed import load_processer
import os
import pytest
import logging
import json
from moto import mock_aws
import boto3
import pyarrow.parquet as pq
from io import BytesIO
from unittest.mock import patch
import pandas as pd
from botocore.exceptions import ClientError

logger = logging.getLogger("test")
logger.setLevel(logging.INFO)
logger.propagate = True

if os.getenv("S3_PROCESS_BUCKET_NAME") == None:
    with open("s3_process_bucket_name.txt") as f:
        S3_BUCKET_NAME = f.readline()
else:
    S3_BUCKET_NAME = os.environ["S3_PROCESS_BUCKET_NAME"]


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

def test_extract_from_parquet_retrieves_data_and_returns_dataframe(s3_client):
    dataf = dataframe_creator('address', file_dict)
    load_processer("address", dataf, "test-bucket")
    result = extract_from_parquet("address", "test-bucket")

    assert isinstance(result, pd.DataFrame)
    assert result.equals(dataf)

def test_input_columns_are_same_as_output_columns_in_dataframe():
    result = extract_from_parquet("dim_design",S3_BUCKET_NAME)
    for col in ["design_name", "file_location", "file_name"]:
        assert col in result.columns
    
def test_extract_from_parquet_raises_client_error_when_no_bucket(
        s3_client, caplog
        ):
    with pytest.raises(ClientError):
        dataf = dataframe_creator('address', file_dict)
        load_processer("address", dataf, "test-bucket")
        extract_from_parquet("address", "no_bucket")
    assert "NoSuchBucket" in caplog.text
