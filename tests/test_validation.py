import pytest
from mdfb.utils.validation import *


def test_validate_directory(mocker):
    mock_dir = "./tests"
    result = validate_directory(mock_dir)
    assert result == mock_dir

def test_validate_directory_no_path(mocker):
    mock_dir = ""
    with pytest.raises(ValueError):
        validate_directory(mock_dir)

def test_validate_limit(mocker):
    mock_limit = "78"
    result = validate_limit(mock_limit)
    assert result == 78

def test_validate_limit_under_1(mocker):
    mock_limit = "0"
    with pytest.raises(ValueError):
        validate_limit(mock_limit)

def test_validate_limit_not_number(mocker):
    mock_limit = "a"
    with pytest.raises(ValueError):
        validate_limit(mock_limit)

def test_validate_did(mocker):
    mock_did = "did:plc:123abc"
    result = validate_did(mock_did)
    assert result == mock_did

def test_validate_did_invalid(mocker):
    mock_did = "dnsadnasndjkl"
    with pytest.raises(ValueError):
        validate_did(mock_did)

def test_validate_threads(mocker):
    mock_threads = "1"
    result = validate_threads(mock_threads)
    assert result == 1

def test_validate_threads_invalid(mocker):
    mock_threads = "a"
    with pytest.raises(ValueError):
        validate_threads(mock_threads)

def test_validate_threads_too_big(monkeypatch):
    mock_threads = "10"
    max_threads = 3
    monkeypatch.setattr("mdfb.utils.validation.MAX_THREADS", max_threads)
    
    result = validate_threads(mock_threads)
    assert result == max_threads

def test_validate_threads_too_little():
    mock_threads = "0"
    with pytest.raises(ValueError):
        validate_threads(mock_threads)
    
    
    