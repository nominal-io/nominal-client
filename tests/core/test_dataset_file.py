import pytest

from nominal.core.dataset_file import filename_from_uri


def test_filename_from_uri_keeps_regular_filename():
    assert filename_from_uri("https://example.com/path/to/file.parquet") == "file.parquet"


def test_filename_from_uri_prevents_encoded_traversal():
    assert filename_from_uri("https://example.com/path/%2e%2e%2fsecret.txt") == "secret.txt"


def test_filename_from_uri_prevents_encoded_absolute_path():
    assert filename_from_uri("https://example.com/path/%2fetc%2fpasswd") == "passwd"


def test_filename_from_uri_rejects_empty_filename():
    with pytest.raises(ValueError, match="safe filename"):
        filename_from_uri("https://example.com/path/")
