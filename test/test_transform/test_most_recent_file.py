from utils.most_recent_file import get_most_recent_key_per_table_from_s3
import pytest
import boto3
from moto import mock_aws
import os
from botocore.exceptions import ClientError
import logging
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


def test_get_s3_raises_error_on_incorrect_table_name(s3_client, caplog):
    with pytest.raises(ClientError):
        get_most_recent_key_per_table_from_s3("nonsense")
    assert "'Result': 'Failure'" in caplog.text


def test_get_s3_gets_file_with_prefix_from_existing_bucket(s3_client):
    s3_client.put_object(
        Bucket="test-bucket",
        Key="table=file/path.json",
        Body=json.dumps({"Key": "Value"}),
    )
    result = get_most_recent_key_per_table_from_s3(
        "file", bucket_name="test-bucket"
    )
    assert result == "table=file/path.json"
    s3_client.put_object(
        Bucket="test-bucket",
        Key="table=file/path2.json",
        Body=json.dumps({"Key": "Value"}),
    )
    result_2 = get_most_recent_key_per_table_from_s3(
        "file", bucket_name="test-bucket"
    )
    assert result_2 == "table=file/path2.json"


def test_get_s3_raises_error_on_incorrect_prefix(s3_client, caplog):
    s3_client.put_object(
        Bucket="test-bucket",
        Key="table=file/path.json",
        Body=json.dumps({"Key": "Value"}),
    )
    with pytest.raises(KeyError):
        get_most_recent_key_per_table_from_s3(
            "nonsense", bucket_name="test-bucket"
        )
    assert "No contents in Prefix or Bucket," in caplog.text