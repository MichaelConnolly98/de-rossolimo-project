from utils.most_recent_pandas import get_s3_file_content_from_key_single
import pytest
import boto3
from moto import mock_aws
import os
from botocore.exceptions import ClientError
import json
from unittest.mock import patch


@pytest.fixture(scope="function")
def aws_cred():
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture()
def s3_client(aws_cred):
    with mock_aws():
        s3 = boto3.client("s3")
        s3.create_bucket(
            Bucket="test-bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        yield s3


def test_s3_file_contents_returns_file_data_from_key_in_list(s3_client):
    key_list = ["table=file/path.json"]
    s3_client.put_object(
        Bucket="test-bucket", Key=key_list[0], Body=json.dumps({"Key": "Value"})
    )

    result = get_s3_file_content_from_key_single(key_list[0], bucket_name="test-bucket")
    assert isinstance(result, list)
    assert result == [{"Key": "Value"}]


def test_get_s3_raises_error_on_incorrect_table_name(s3_client, caplog):
    with pytest.raises(ClientError):
        get_s3_file_content_from_key_single("nonsense")
    assert "'Result': 'Failure'" in caplog.text
