"""
-------------------------------------------------------------------------------
Copyright (C) 2026 Hussein Adi

This program is free software, and may be used and distributed under the terms
of the included Hippocratic License. You should have received a copy of this
license. If not, see https://github.com/ehrenamt/basicetl/blob/main/LICENSE.md
and the Hippocratic License at https://firstdonoharm.dev/

-------------------------------------------------------------------------------
Filename: extract.py

Functions that abstract the extraction logic in the basicetl class.

Provides functions for pulling from local or remote sources with different data
formats and returns them as pandas dataframes. Defines helper functions for
these tasks as well.
-------------------------------------------------------------------------------
"""


# Python standard library imports
import io
import os
from pathlib import Path
from urllib.error import URLError, HTTPError
from urllib.parse import urlparse, urlunparse
from urllib.request import urlopen, Request

# PyPI imports
import pandas as pd
from pandas.errors import ParserError

# Project imports
from basicetl.etl_options import ExtractOptions
from basicetl.etl_types import SourceType, DataType


# logger = logging.getLogger(__name__)


def extract_based_on_source(
    caller_path: str,
    data_source: str,
    options: object = None
) -> pd.DataFrame:
    """
    Accepts a source, checks and validates it, and returns a dataframe.

    If the source is a path, it must either be an absolute path OR
    a relative path, relative to the current working directory.
    """

    filetype = ""
    df = None

    if not isinstance(options, ExtractOptions):
        return None

    source_type = get_source_type(caller_path, data_source)

    if source_type == SourceType.URL:

        source = convert_to_https(data_source)

        try:

            df = extract_web_source(url=source)

            return df

        except HTTPError as e:
            print(f'HTTP Error: {e.code} - {e.reason}')

        except URLError as e:
            print(f'URL Error: {e.reason}')

        except Exception as e:
            print(f'Unexpected error: {e}')

    elif source_type == SourceType.FILE:

        source_path = data_source

        if not Path(data_source).is_absolute():
            source_path = os.path.join(caller_path, data_source)

        filetype = Path(source_path).suffix.lower()

        df = read_local_file(source_path, filetype)

    return df


def get_source_type(caller_source, source) -> SourceType:
    """Returns an enum indicating the type."""

    if isinstance(source, str):

        if is_valid_url(source):

            return SourceType.URL

        if is_valid_path(caller_source, source):

            return SourceType.FILE

        raise ValueError(
            f"Invalid source string: {source}. "
            "Must provide a valid URL or path."
            )

    raise ValueError(
        f"Invalid argument: {source}. "
        "Must provide a string to URL / FILE."
        )


def read_local_file(path: str, filetype: DataType) -> pd.core.frame.DataFrame:
    """Case-based file reading helper for csv, json, and xml."""

    df = None

    # This may not be the shortest or quickest way to do this.
    # I am prioritizing readability.
    match filetype:

        case ".csv":
            try:
                df = pd.read_csv(path, header=0)

            except FileNotFoundError as err:
                print(f"File not found: {err}")

            except ParserError as err:
                print(f'Could not parse CSV: {err}')

            except Exception as err:
                print(f'Unexpected error: {err}')

        case ".json":

            try:
                df = pd.read_json(path)

            except FileNotFoundError as err:
                print(f"File not found: {err}")

        case ".xml":
            try:
                df = pd.read_xml(path)
            except FileNotFoundError as err:
                print(f"File not found: {err}")

        # default case, nothing happens
        case _:

            pass

    return df


# We only expect to call this on validated URLs.
def convert_to_https(url: str) -> str:
    """
    Converts valid HTTP urls to HTTPS. We only expect to call this on validated
    URLs, hence we may forgo robust checks.
    """

    parsed = urlparse(url)
    parsed = parsed._replace(scheme='https')
    return urlunparse(parsed)


def is_valid_url(source: str) -> bool:

    source = source.strip()

    try:
        parsed = urlparse(source)
        if parsed.scheme in ('http', 'https', 'ftp') and parsed.netloc:
            return True

        return False

    except Exception:
        return False


def is_valid_path(caller_path, path: str) -> bool:

    path = Path(path)

    if (path.exists() and path.is_file()):
        return True

    fullpath = Path(os.path.join(caller_path, path))

    print(f'checking fullpath {fullpath}')

    return (fullpath.exists() and fullpath.is_file())


def extract_web_source(url: str) -> pd.DataFrame:
    """Case-based extraction of remote files into DataFrames."""

    df = None

    web_request = Request(url)

    with urlopen(web_request) as web_response:
        web_content_type = web_response.headers.get('Content-Type', '')
        web_content = web_response.read()

        # not a robust check, keeping it simple
        if 'html' in web_content_type.lower():
            raise ValueError(
                "Received HTML content,"
                "not a valid data file (csv, json, xml)."
            )

        filetype = web_content_type.split(';')[0].strip().lower()

        match filetype:
            case "text/csv":
                df = pd.read_csv(io.StringIO(
                    web_content.decode('utf-8', errors='replace')))

            case 'application/json':
                df = pd.read_json(
                    io.StringIO(web_content.decode('utf-8', errors='replace'))
                    )

            case 'application/xml' | 'text/xml':
                df = pd.read_xml(
                    io.StringIO(web_content.decode('utf-8', errors='replace')))

            case _:
                # log please
                print("Inapplicable filetype.")
                raise ValueError("Invalid filetype.")

    return df
