import pytest
import boto3
from moto import mock_aws
import os
from utils.log_time import get_timestamp_from_logs, InvalidInput
import logging
from botocore.exceptions import ClientError
from unittest.mock import patch
from datetime import datetime as dt
 
#PutLogEvents timestamps will be rejected if more than 2 weeks old.
#This is a workaround to use the current year, month, day only

now_time = dt.now()
date = dt(now_time.year, now_time.month, now_time.day, 0, 0, 0)
date_unix = (int(date.timestamp()*1000))
date_str = str(date)


@pytest.fixture(scope="function")
def aws_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


# mock object with no streaming logs or log groups
@pytest.fixture(scope="function")
def mock_logs_client(aws_credentials):
    with mock_aws():
        yield boto3.client("logs", region_name="eu-west-2")


# mock object with streaming logs and log group
@pytest.fixture(scope="function")
def mock_logs_with_stream_and_group(mock_logs_client):
    mock_logs_client.create_log_group(logGroupName="string")

    mock_logs_client.create_log_stream(logGroupName="string", logStreamName="string")
    yield mock_logs_client



def test_get_timestamp_returns_timestamp_from_logs(mock_logs_with_stream_and_group):
    result1 = mock_logs_with_stream_and_group.put_log_events(
        logGroupName="string",
        logStreamName="string",
        logEvents=[
            {"timestamp": date_unix, "message": "string"},
        ],
        sequenceToken="string",
    )
    print(result1)
    result = get_timestamp_from_logs("string")
    assert result == date_str


def test_get_timestamp_returns_first_log_stream_start_time(
    mock_logs_with_stream_and_group,
):
    mock_logs_with_stream_and_group.put_log_events(
        logGroupName="string",
        logStreamName="string",
        logEvents=[
            {"timestamp": 1723542891807, "message": "string"},
        ],
        sequenceToken="string",
    )
    mock_logs_with_stream_and_group.put_log_events(
        logGroupName="string",
        logStreamName="string",
        logEvents=[
            {"timestamp": date_unix, "message": "string"},
        ],
        sequenceToken="string",
    )
    result = get_timestamp_from_logs("string")
    assert result == date_str


def test_get_timestamp_returns_from_latest_log_stream(mock_logs_with_stream_and_group):

    mock_logs_with_stream_and_group.put_log_events(
        logGroupName="string",
        logStreamName="string",
        logEvents=[
            {"timestamp": 1723542891807, "message": "string"},
        ],
        sequenceToken="string",
    )
    mock_logs_with_stream_and_group.create_log_stream(
        logGroupName="string", logStreamName="second_stream"
    )
    mock_logs_with_stream_and_group.put_log_events(
        logGroupName="string",
        logStreamName="second_stream",
        logEvents=[
            {"timestamp": date_unix, "message": "string"},
        ],
        sequenceToken="string",
    )
    result = get_timestamp_from_logs("string")
    assert result == date_str


def test_get_timestamp_raises_client_error_when_resource_not_exists(
    mock_logs_client, caplog
):
    with pytest.raises(ClientError):
        get_timestamp_from_logs()
        assert "ResourceNotFoundException" in caplog.text


def test_get_timestamp_catches_invalid_input_exception(mock_logs_client, caplog):
    with caplog.at_level(logging.ERROR):
        with pytest.raises(InvalidInput):
            get_timestamp_from_logs(log_group_name=None)
    assert "log_group_name parameter" in caplog.text


@patch("utils.log_time.boto3.client", side_effect=Exception)
def test_get_timestamp_catches_other_errors(patch_client, caplog):
    with pytest.raises(Exception):
        get_timestamp_from_logs()
    assert "'Result': 'Failure'" in caplog.text
