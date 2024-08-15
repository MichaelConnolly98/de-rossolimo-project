from src.extract.extract_data import get_db_credentials
from botocore.exceptions import ClientError

import pytest
import boto3
import json
from moto import mock_aws
import os
import logging
from unittest.mock import patch


@pytest.fixture(scope="function")
def aws_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope="function")
def mock_sm_client(aws_credentials):
    with mock_aws():
        sm = boto3.client("secretsmanager")
        secret_info = {
            "username": "name",
            "password": "pw",
            "engine": "2-cylinder",
            "host": "jeeves",
            "port": "1111",
            "dbname": "database",
        }
        secret_info_str = json.dumps(secret_info)
        sm.create_secret(Name="secret", SecretString=secret_info_str)
        yield sm



def test_all_dict_keys_available(mock_sm_client):
    # secret_info = {
    #     "username": "name",
    #     "password": "pw",
    #     "engine": "2-cylinder",
    #     "host": "jeeves",
    #     "port": "1111",
    #     "dbname": "database",
    # }
    # secret_info_str = json.dumps(secret_info)
    # response = mock_sm_client.create_secret(Name="secret", SecretString=secret_info_str)
    # print(response)
    # assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
    result = get_db_credentials("secret", sm_client=mock_sm_client)
    dict_keys = ["username", "password", "engine", "host", "port", "dbname"]
    for key in dict_keys:
        assert key in list(result.keys())


def test_error_handling_secret_name_not_found(mock_sm_client, caplog):
    with caplog.at_level(logging.ERROR):
        with pytest.raises(ClientError) as e:
            result = get_db_credentials("does_not_exist", sm_client=mock_sm_client)
            assert (
                str(e.value)
                == "An error occurred (ResourceNotFoundException) when calling the GetSecretValue operation: Secrets Manager can't find the specified secret."
            )
            assert (
                "An error occurred (ResourceNotFoundException) when calling the GetSecretValue operation: Secrets Manager can't find the specified secret."
                in caplog.text
            )
