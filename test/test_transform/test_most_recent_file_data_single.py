from utils.most_recent_pandas import file_data_single
from unittest.mock import patch
import pytest


@patch("utils.most_recent_pandas.get_table_names_single")
@patch("utils.most_recent_pandas.get_most_recent_key_per_table_from_s3_single")
@patch("utils.most_recent_pandas.get_s3_file_content_from_key_single")
def test_file_data_single_returns_dict(s3_file_content, s3_keys, table_names):
    s3_file_content.return_value = [{"address": ["Hello"]}]
    s3_keys.return_value = "1"
    table_names.return_value = {"address": []}
    assert type(file_data_single()) == dict


@patch("utils.most_recent_pandas.get_table_names_single")
@patch("utils.most_recent_pandas.get_most_recent_key_per_table_from_s3_single")
@patch("utils.most_recent_pandas.get_s3_file_content_from_key_single")
def test_file_data_single_invokes_sub_functions(s3_file_content, s3_keys, table_names):
    s3_file_content.return_value = [{"address": ["Hello"]}]
    s3_keys.return_value = ["1", "2"]
    table_names.return_value = {"address": []}
    file_data_single()
    s3_file_content.assert_called_once()
    s3_keys.assert_called_once()
    table_names.assert_called_once()
    s3_file_content.assert_called_with(["1", "2"])


@patch("utils.most_recent_pandas.get_table_names_single")
@patch("utils.most_recent_pandas.get_most_recent_key_per_table_from_s3_single")
@patch("utils.most_recent_pandas.get_s3_file_content_from_key_single")
def test_file_data_single_returns_dict_with_expected_properties(
    s3_file_content, s3_keys, table_names
):
    s3_file_content.return_value = [{"address": ["Hello"]}]
    s3_keys.return_value = ["1", "2"]
    table_names.return_value = {"address": []}
    result = file_data_single()
    assert result == {"address": ["Hello"]}


@patch("utils.most_recent_pandas.get_table_names_single")
@patch("utils.most_recent_pandas.get_most_recent_key_per_table_from_s3_single")
@patch("utils.most_recent_pandas.get_s3_file_content_from_key_single")
def test_file_data_single_combines_table_name_result_with_longer_length_input(
    s3_file_content, s3_keys, table_names
):
    s3_file_content.return_value = [{"address": ["Hello"]}, {"address": ["Goodbye"]}]
    s3_keys.return_value = ["1", "2"]
    table_names.return_value = {"address": []}
    result = file_data_single()
    assert result == {"address": ["Hello", "Goodbye"]}


@patch("utils.most_recent_pandas.get_table_names_single")
@patch("utils.most_recent_pandas.get_most_recent_key_per_table_from_s3_single")
@patch("utils.most_recent_pandas.get_s3_file_content_from_key_single")
def test_file_data_single_works_for_multiple_table_names(
    s3_file_content, s3_keys, table_names
):
    s3_file_content.side_effect = [
        [{"address": ["Hello"]}, {"address": ["Goodbye"]}],
        [{"design": ["Hello"]}, {"design": ["Goodbye"]}],
    ]
    s3_keys.side_effect = [["1", "2"], ["3", "4"]]
    table_names.return_value = ["address", "design"]

    result = file_data_single()
    assert result == {"address": ["Hello", "Goodbye"], "design": ["Hello", "Goodbye"]}
