import pytest
import boto3
from moto import mock_aws
import os
from src.extract.log_time import get_timestamp_from_logs, InvalidInput
import logging
from botocore.exceptions import ClientError
from unittest.mock import patch


# put fake logs in, see if it returns the most recent one
# see if the time works with an sql query (all times are after the input time)
# client error handling of course
# other error handling


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
    mock_logs_with_stream_and_group.put_log_events(
        logGroupName="string",
        logStreamName="string",
        logEvents=[
            {"timestamp": 1723542891807, "message": "string"},
        ],
        sequenceToken="string",
    )
    result = get_timestamp_from_logs("string")
    assert result == "2024-08-13 10:54:51"


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
            {"timestamp": 1723542899807, "message": "string"},
        ],
        sequenceToken="string",
    )
    result = get_timestamp_from_logs("string")
    assert result == "2024-08-13 10:54:51"


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
            {"timestamp": 1723549999999, "message": "string"},
        ],
        sequenceToken="string",
    )
    result = get_timestamp_from_logs("string")
    assert result == "2024-08-13 12:53:19"


def test_get_timestamp_raises_client_error_when_resource_not_exists(mock_logs_client, caplog):
    with pytest.raises(ClientError):
        get_timestamp_from_logs()
        assert "ResourceNotFoundException" in caplog.text

def test_get_timestamp_catches_invalid_input_exception(mock_logs_client, caplog):
    with caplog.at_level(logging.ERROR):
        with pytest.raises(InvalidInput):
            get_timestamp_from_logs(log_group_name=None)
    assert "log_group_name parameter" in caplog.text

@patch('src.extract.log_time.boto3.client', side_effect=Exception)
def test_get_timestamp_catches_other_errors(patch_client, caplog):
    with pytest.raises(Exception):
        get_timestamp_from_logs()
    assert "'Result': 'Failure'" in caplog.text
