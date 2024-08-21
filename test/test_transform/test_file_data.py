from src.transform.pandas_testing import file_data
from unittest.mock import patch
import pytest

@patch("src.transform.pandas_testing.get_table_names")
@patch("src.transform.pandas_testing.get_keys_from_s3")
@patch("src.transform.pandas_testing.get_s3_file_content_from_keys")
def test_file_data_returns_dict(s3_file_content, s3_keys, table_names):
    s3_file_content.return_value = [{"address": ["Hello"]}]
    s3_keys.return_value = ["1", "2"]
    table_names.return_value = {"address": []}
    assert type(file_data("pandas_test_data.json")) == dict

@patch("src.transform.pandas_testing.get_table_names")
@patch("src.transform.pandas_testing.get_keys_from_s3")
@patch("src.transform.pandas_testing.get_s3_file_content_from_keys")
def test_file_data_invokes_sub_functions(
    s3_file_content, s3_keys, table_names
    ):
    s3_file_content.return_value = [{"address": ["Hello"]}]
    s3_keys.return_value = ["1", "2"]
    table_names.return_value = {"address": []}
    file_data("pandas_test_data.json")
    s3_file_content.assert_called_once()
    s3_keys.assert_called_once()
    table_names.assert_called_once()
    s3_file_content.assert_called_with(["1", "2"])

@patch("src.transform.pandas_testing.get_table_names")
@patch("src.transform.pandas_testing.get_keys_from_s3")
@patch("src.transform.pandas_testing.get_s3_file_content_from_keys")
def test_file_data_returns_dict_with_expected_properties(
    s3_file_content, s3_keys, table_names
    ):
    s3_file_content.return_value = [{"address": ["Hello"]}]
    s3_keys.return_value = ["1", "2"]
    table_names.return_value = {"address": []}
    result = file_data("pandas_test_data.json")
    assert result == {"address": ["Hello"]}

@patch("src.transform.pandas_testing.get_table_names")
@patch("src.transform.pandas_testing.get_keys_from_s3")
@patch("src.transform.pandas_testing.get_s3_file_content_from_keys")
def test_file_data_combines_table_name_result_with_longer_length_input(
    s3_file_content, s3_keys, table_names
):
    s3_file_content.return_value = [
        {"address": ["Hello"]}, {"address": ["Goodbye"]}
        ]
    s3_keys.return_value = ["1", "2"]
    table_names.return_value = {"address": []}
    result = file_data("pandas_test_data.json")
    assert result == {'address': ['Hello', 'Goodbye']}

@patch("src.transform.pandas_testing.get_table_names")
@patch("src.transform.pandas_testing.get_keys_from_s3")
@patch("src.transform.pandas_testing.get_s3_file_content_from_keys")
def test_file_data_works_for_multiple_table_names(
    s3_file_content, s3_keys, table_names
        ):
    s3_file_content.side_effect = [
        [{"address": ["Hello"]}, {"address": ["Goodbye"]}],
        [{"design": ["Hello"]}, {"design": ["Goodbye"]}] ]
    s3_keys.side_effect = [["1", "2"],["3", "4"]]
    table_names.return_value = ["address", "design"]

    result = file_data("pandas_test_data.json")
    assert result == {
        'address': ['Hello', 'Goodbye'], 'design': ['Hello', 'Goodbye']
        }

@patch("src.transform.pandas_testing.get_table_names")
@patch("src.transform.pandas_testing.get_keys_from_s3")
@patch("src.transform.pandas_testing.get_s3_file_content_from_keys")
def test_file_data_raises_exceptions(
        s3_file_content, s3_keys, table_names, caplog
        ):
    table_names.side_effect = Exception
    with pytest.raises(Exception):
        file_data("pandas_test_data.json")
    assert "'Result': 'Failure'" in caplog.text
