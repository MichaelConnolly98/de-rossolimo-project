from src.load import load
from moto import mock_aws
import pytest
import boto3
import os
import logging
from unittest.mock import patch

logger = logging.getLogger('test')
logger.setLevel(logging.INFO)
logger.propagate = True

@pytest.fixture()
def aws_creds():
    os.environ["AWS_ACCESS_KEY_ID"] = 'test'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'test'
    os.environ['AWS_SECURITY_TOKEN'] = 'test'
    os.environ['AWS_SESSION_TOKEN'] = 'test'
    os.environ['AWS_DEFAULT_REGION'] = 'eu-west-2'
    os.environ['S3_BUCKET_NAME'] = 'test-bucket'

@pytest.fixture()
def s3_client(aws_creds):
    with mock_aws():
        s3 = boto3.client('s3')
        s3.create_bucket(Bucket='test-bucket',
            CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'})
        yield s3

def test_func_loads_object_and_logs(s3_client, caplog):
    assert load({"fake_column": "fake_data"}) == {'result': 'success'}
    with caplog.at_level(logging.INFO):
        load({'fake': 'Data'})
        assert 'success' in caplog.text

@patch('src.load.BUCKETNAME', side_effect='fake_bucket')
def test_func_raises_exception_and_logs(s3_client, caplog):
    with caplog.at_level(logging.INFO):
        assert load(data=None) == Exception
        assert 'error' in caplog.text