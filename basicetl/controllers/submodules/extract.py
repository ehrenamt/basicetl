"""
Functions that abstract the extraction logic in the basicetl class.

Provides functions for pulling from local or remote sources with different data formats
and returns them as pandas dataframes.

Functions:
- extract_based_on_source(source): Identifies, validates, and extracts the data source
- is_valid_url(source): Returns true if the URL is a valid string.
- is_valid_path(path): Returns true if the path is a valid path and the file exists at the path.

"""

from enum import Enum, auto
import io
import logging
import os
from pathlib import Path
from urllib.error import URLError, HTTPError
from urllib.parse import urlparse, urlunparse
from urllib.request import urlopen, Request
# zipfile

import pandas as pd
from pandas.errors import ParserError
import sqlalchemy

from basicetl.controllers.extract_options import ExtractOptions


class SourceType(Enum):
    """Represents source type during extraction."""

    URL = auto()
    FILE = auto()
    SQLCONN = auto()


class DataType(Enum):
    """Represents data format/type during extraction."""

    CSV = auto()
    JSON = auto()
    XML = auto()

logger = logging.getLogger(__name__)


def extract_based_on_source(source: str, options: object) -> pd.core.frame.DataFrame:
    """Accepts a source, checks and validates it, and returns a dataframe."""

    filetype = ""
    df = None

    if not isinstance(options, ExtractOptions):
        return None

    script_dir = os.path.dirname(os.path.abspath(__file__))
    source_path = os.path.join(script_dir, source)

    logger.debug(f'Source points to {source}.')

    source_type = get_source_type(source=source)

    if source_type == SourceType.URL:

        source = convert_to_https(source)

        try:

            web_request = Request(source)

            with urlopen(web_request) as web_response:
                web_content_type = web_response.headers.get('Content-Type', '')
                web_content = web_response.read()

                # not a robust check, keeping it simple
                if 'html' in web_content_type.lower():
                    raise ValueError("Received HTML content, not valid data file.")

                filetype = web_content_type.split(';')[0].strip().lower()

                match filetype:
                    case "text/csv":
                        df = pd.read_csv(io.StringIO(web_content.decode('utf-8', errors='replace')))

                    case 'application/json':
                        df = pd.read_json(
                            io.StringIO(web_content.decode('utf-8', errors='replace'))
                            )

                    case 'application/xml' | 'text/xml':
                        df = pd.read_xml(io.StringIO(web_content.decode('utf-8', errors='replace')))

                    case _:
                        # log please
                        print("Inapplicable filetype.")
                        raise ValueError("Invalid filetype.")

                return df

        except HTTPError as e:
            logger.error(f'HTTP Error: {e.code} - {e.reason}')

        except URLError as e:
            logger.error(f'URL Error: {e.reason}')

        except Exception as e:
            logger.error(f'Unexpected error: {e}')

    elif source_type == SourceType.FILE:

        filetype = Path(source_path).suffix.lower()

        df = read_file(source_path, filetype)

    elif source_type == SourceType.SQLCONN:
        connection_string = source["connection_string"]
        query = source["query"]

        return extract_from_sql(connection_string, query)

    return df


def get_source_type(source) -> SourceType:
    """Accepts a string or dict and returns an enum indicating the type."""

    # special case for SQL
    if isinstance(source, dict):

        if is_valid_sql_conn(source):
            return SourceType.SQLCONN

        raise ValueError(
            f"Invalid source string: {source}. "
            "Source identified as SQL connection, but this connection is invalid."
            )

    if isinstance(source, str):

        if is_valid_url(source):

            logger.debug('Source provided passes is_valid_URL() function.')

            return SourceType.URL

        if is_valid_path(source):

            return SourceType.FILE

        raise ValueError(
            f"Invalid source string: {source}. "
            "Must provide a valid URL, path, or SQL connection."
            )

    raise ValueError(
        f"Invalid argument: {source}. "
        "Must provide a string to URL / FILE or dictionary representing an SQL connection."
        )


def read_file(path: str, filetype) -> pd.core.frame.DataFrame:
    """Case-based file reading helper."""

    df = None

    # This may not be the shortest or quickest way to do this.
    # I am prioritizing readability.
    match filetype:

        case ".csv":
            try:
                df = pd.read_csv(path)

            except FileNotFoundError as e:
                print(f"File not found: {e}")

            except ParserError as e:
                logger.error(f'Could not parse CSV: {e}')

            except Exception as e:
                logger.error(f'Unexpected error: {e}')

        case ".json":

            try:
                df = pd.read_json(path)

            except FileNotFoundError as e:
                print(f"File not found: {e}")

        case ".xml":
            try:
                df = pd.read_xml(path)
            except FileNotFoundError:
                print(f"File not found: {e}")

        case _:
            # print(f'Default case, nothing happens.')
            logger.debug(f'{path} default')

    return df


def extract_from_sql(connection_string, query):

    try:
        engine = sqlalchemy.create_engine(connection_string)
        return pd.read_sql(query, engine)
    
    except Exception as e:
        raise ValueError(f"Error extracting from SQL: {e}")


# We only expect to call this on validated URLs.
def convert_to_https(url: str) -> str:
    """Converts valid HTTP urls to HTTPS."""

    parsed = urlparse(url)
    parsed = parsed._replace(scheme='https')
    return urlunparse(parsed)


def is_valid_url(source: str) -> bool:

    try:
        parsed = urlparse(source)
        if parsed.scheme in ('http', 'https', 'ftp') and parsed.netloc:
            return True

        return False

    except Exception:
        return False


def is_valid_path(path: str) -> bool:

    try:
        path = Path(path)
        return path.exists() and path.is_file()

    except Exception:
        return False


def is_valid_sql_conn(source: dict) -> bool:
    """
    Validates both the connection string and query of an SQL source.
    Currently just checks to ensure dictionary size is right, as for now,
    we are keeping this tool simple.
    """

    if len(source.items()) != 2:
        return False

    conn_str_valid = (
            source.startswith("postgresql://") or
            source.startswith("mysql://") or
            source.startswith("postgresql+pg8000://") or
            source.startswith("mysql+pymysql://")
        )

    query_valid = True

    return (conn_str_valid and query_valid)
