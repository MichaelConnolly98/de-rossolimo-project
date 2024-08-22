from src.transform.load_processed import load_processer
from src.transform.pandas_testing import dataframe_creator
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

@patch('src.transform.load_processed.datetime')
def test_func_transforms_to_parquet(datetime_patch, s3_client, caplog):
     with caplog.at_level(logging.INFO):
        datetime_patch.now.return_value = datetime(2002, 11, 9, 16, 38, 23)
        dataf = dataframe_creator('address', file_dict)
        result = load_processer(dataf, "test-bucket")
        
        #get object just loaded in
        s3_object = s3_client.get_object(Bucket='test-bucket', Key='table=address/year=2002/month=11/day=9/16:38:23.parquet')
        #read bytes
        buffer = BytesIO(s3_object['Body'].read())
        #convert to table
        table = pq.read_table(buffer)
        #convert to dataframe
        df = table.to_pandas()
        #assert dataframe from s3 bucket is same as dataframe passed to function
        assert df.equals(dataf)
        assert "data uploaded at" in caplog.text
        assert result == {"Result": "Success", "Message": "data uploaded"}

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
            load_processer(fake_data)
            assert {'Result': "Failure", 'Error': "AttributeError occurred:"} in caplog.text

@patch('src.transform.load_processed.datetime')
def test_is_saved_into_s3_in_correct_file_structure(datetime_patch, s3_client):
    datetime_patch.now.return_value = datetime(2002, 11, 9, 16, 38, 23)
    dataf = dataframe_creator('address', file_dict)
    load_processer(dataf, "test-bucket")

    s3_object_list = s3_client.list_objects_v2(Bucket='test-bucket')
    assert s3_object_list['Contents'][0]['Key'] == 'table=address/year=2002/month=11/day=9/16:38:23.parquet'


def test_no_data_is_uploaded_when_passed_none(s3_client, caplog):
    with caplog.at_level(logging.INFO):
        assert load_processer(None) == {"Message": "no data to upload"}
        assert "no data to upload" in caplog.text