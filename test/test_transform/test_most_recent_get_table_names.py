from utils.most_recent_pandas import get_table_names_single
from unittest.mock import Mock, patch
import pytest
from pg8000.exceptions import DatabaseError


def test_get_table_names_single_returns_list_of_table_names():
    result = get_table_names_single()
    assert isinstance(result, list)
    assert result == [
        "address",
        "staff",
        "payment",
        "department",
        "transaction",
        "currency",
        "payment_type",
        "sales_order",
        "counterparty",
        "purchase_order",
        "design",
    ]


def test_get_table_names_single_raises_exceptions_and_logs_errors(caplog):
    with pytest.raises(Exception):
        get_table_names_single(connection="test")
    assert "'Result': 'Failure'" in caplog.text


@patch("utils.pandas_testing.get_connection")
def test_get_table_names_single_filters_out_prisma_migrations(patch_connection):
    mock_conn = Mock()
    mock_conn.run.return_value = [["table1"], ["table2"], ["_prisma_migrations"]]
    patch_connection.return_value = mock_conn
    result = get_table_names_single(connection=patch_connection)
    assert result == ["table1", "table2"]


def test_get_table_names_single_invokes_connection_function():
    connection = Mock()
    with pytest.raises(Exception):
        get_table_names_single(connection=connection)
    connection.assert_called_once()


@patch("utils.pandas_testing.get_connection")
def test_get_table_names_single_catches_and_logs_database_errors(
    connection_patch, caplog
):
    mock_conn = Mock()
    mock_conn.run.side_effect = DatabaseError
    connection_patch.return_value = mock_conn
    with pytest.raises(DatabaseError):
        get_table_names_single(connection=connection_patch)

    mock_conn.close.assert_called_once()
    assert "'Error': 'A database error has occured" in caplog.text
