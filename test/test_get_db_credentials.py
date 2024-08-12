from src.get_db_credentials import get_db_credentials
from botocore.exceptions import ClientError

import pytest

def test_all_dict_keys_available():
    result = get_db_credentials("totesys")
    dict_keys = ['username', 'password', 'engine', 'host', 'port', 'dbname']
    for key in dict_keys:
        assert key in list(result.keys())

def test_error_handling_secret_name_not_found():
    
    with pytest.raises(ClientError) as e: 
        result = get_db_credentials("does_not_exist")
    assert str(e.value) == "An error occurred (ResourceNotFoundException) when calling the GetSecretValue operation: Secrets Manager can't find the specified secret."  
    