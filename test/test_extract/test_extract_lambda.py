from unittest.mock import patch

from src.extract.extract_lambda import lambda_handler
import logging
import os
import pytest


"""possible tests:
mock extract func to return exception
make of other functions, check invocation?
do I need to put params in to mock somehow? 
need to patch basically all the functions within - multiple patches in one
(this allowed? - yes, @ @ @ @ multiple times)
"""

with open("s3_bucket_name.txt") as file:
    os.environ["S3_BUCKET_NAME"] = file.readline()


@patch("src.extract.extract_lambda.extract_func")
@patch("src.extract.extract_lambda.get_timestamp_from_logs")
@patch("src.extract.extract_lambda.load", side_effect=Exception)
def test_lambda_handler_catches_exceptions(
    load_mock, timestamp_mock, extract_mock, caplog
):
    with pytest.raises(Exception):
        lambda_handler(0, 0)
    assert "Unexpected Exception occured in lambda_handler" in caplog.text


@patch("src.extract.extract_lambda.extract_func")
@patch("src.extract.extract_lambda.get_timestamp_from_logs")
@patch("src.extract.extract_lambda.load")
def test_lambda_handler_invokes_sub_functions_with_expected_values(
    load_mock, timestamp_mock, extract_mock, caplog
):
    timestamp_mock.return_value = "2000:00:00 00:00:00"
    extract_mock.return_value = "test"

    lambda_handler(0, 0)

    extract_mock.assert_called_once()
    timestamp_mock.assert_called_once()
    load_mock.assert_called_once()
    extract_mock.assert_called_with("2000:00:00 00:00:00")
    load_mock.assert_called_with("test")
    assert "Extract Lambda ran successfully" in caplog.text
