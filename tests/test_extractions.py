"""Unit tests and mock tests for submodule.extract."""

# pylint: disable=missing-docstring

import pytest
from basicetl.controllers.submodules import extract as ex
# from unittest.mock import patch


@pytest.mark.parametrize("input_http_url, expected_https_url", [
    ("http://example.com", "https://example.com"),
    ("https://example.com", "https://example.com"),
    ("ftp://example.com/path", "https://example.com/path"),
])

def test_convert_to_https(input_http_url, expected_https_url):
    assert ex.convert_to_https(input_http_url) == expected_https_url


@pytest.mark.parametrize("input_http_url, expected_bool_output", [
    ("https://people.sc.fsu.edu/~jburkardt/data/csv/cities.csv", True),
    ("invalid://people.sc.fsu.edu/~jburkardt/data/csv/cities.csv", False),
    ("https:/mistake//example/com", False),
    ("http://example.com", True),
    ("https://example.com", True),
])

def test_is_valid_url(input_http_url, expected_bool_output):
    assert ex.is_valid_url(input_http_url) == expected_bool_output


@pytest.mark.parametrize("input_file_path, expected_bool_output", [
    ("test/data/datafile_schema1_1.json", True),
    ("data/datafile_schema1_1", False),
])

def test_is_valid_path(input_file_path, expected_bool_output):

    assert ex.is_valid_path(input_file_path) == expected_bool_output


@pytest.mark.parametrize("source, expected_source_type", [
    ("test/data/datafile_schema1_1.json", ex.SourceType.FILE),
    ("test/data/file.xml", ex.SourceType.FILE),
    ("https://example.com", ex.SourceType.URL),
])

def test_get_source_type(source, expected_source_type):
    assert ex.get_source_type(source) == expected_source_type
    return
