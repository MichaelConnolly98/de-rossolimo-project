from utils.load_data import load
from moto import mock_aws
import pytest
import boto3
import os
import logging
from unittest.mock import patch
from datetime import datetime

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


def test_func_loads_object_and_logs(s3_client, caplog):
    with caplog.at_level(logging.INFO):
        assert load({"all_data": {"fake": ["Data"]}}) == {
            "Result": "Success",
            "Message": "data uploaded",
        }
        assert "'Result': 'Success', 'Message': " in caplog.text


@patch("utils.load_data.boto3.client", side_effect=Exception)
def test_func_raises_exception_and_logs(patch_client, caplog):
    with caplog.at_level(logging.INFO):
        with pytest.raises(Exception):
            load({"all_data": {"fake": ["Data"]}})
        "Exception occurred on upload to s3 bucket" in caplog.text


@patch("utils.load_data.datetime")
def test_func_logs_correct_time(datetime_patch, s3_client, caplog):
    datetime_patch.now().return_value = "2002-11-09T16:38:23.417667"
    datetime_patch.now.return_value.strftime.side_effect = ["2002-11-09", "16:38:23"]
    with caplog.at_level(logging.INFO):
        load({"all_data": {"fake": ["Data"]}})
    assert "2002-11-09" in caplog.text


def test_func_splits_data_by_table(s3_client):
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
    load(fake_data)
    response = s3_client.list_objects_v2(Bucket="test-bucket")
    print(response)
    counter = 0
    for _ in response["Contents"]:
        counter += 1
    assert response["KeyCount"] == 2
    assert counter == 2
    assert (
        f"table=table1/year={datetime.now().year}/month={datetime.now().month}/day={datetime.now().day}"
    ) in response["Contents"][0]["Key"]
    assert (
        f"table=table2/year={datetime.now().year}/month={datetime.now().month}/day={datetime.now().day}"
    ) in response["Contents"][1]["Key"]


def test_func_can_log_when_empty_dict_body_uploaded(s3_client, caplog):
    with caplog.at_level(logging.INFO):
        fake_data = {
            "all_data": {
                "table1": [],
                "table2": [],
                "table3": [],
                "table4": [],
                "table5": [],
                "table6": [],
                "table7": [],
                "table8": [],
                "table9": [],
                "table10": [],
                "table11": [],
            }
        }
        load(fake_data)
    assert "All tables will be uploaded as empty files" in caplog.text


def test_func_can_handle_non_serializable_objects(s3_client, caplog):
    with caplog.at_level(logging.INFO):
        fake_data = {"all_data": {"table1": [{"nso": datetime(2002, 8, 14)}]}}
        result = load(fake_data)
    assert result == {"Result": "Success", "Message": "data uploaded"}
