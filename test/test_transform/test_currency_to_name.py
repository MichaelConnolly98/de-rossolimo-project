from utils.currency_code_to_name import currency_code_to_name
import pytest

def test_valid_currency_code_returns_name():
    name = currency_code_to_name('USD')
    assert name == 'United States dollar'

def test_invalid_currency_code_returns_KeyError():
    with pytest.raises(KeyError):
        name = currency_code_to_name('does_not_exist')