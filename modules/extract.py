#
# extract.py
#
# Provides functions for pulling from local or remote sources with different data formats 
# and returns them as pandas dataframes.
# 
# Functions:
#    extract_based_on_source(source): Identifies, validates, and extracts the data source
#   is_valid_url(source): Returns true if the URL is a valid string.
#   is_valid_path(path): Returns true if the path is a valid path and the file exists at the path.
#

import io
import logging
import os
import pandas as pd
from pathlib import Path
from urllib.error import URLError, HTTPError
from urllib.parse import urlparse
from urllib.request import urlopen, Request

logger = logging.getLogger(__name__)

def extract_based_on_source(source: str, output='df') -> pd.core.frame.DataFrame:

    filetype = ""
    df = None

    script_dir = os.path.dirname(os.path.abspath(__file__))
    source_path = os.path.join(script_dir, source) 

    logger.debug(f'Source points to {source}.')

    if is_valid_url(source):

        logger.debug('Source provided passes is_valid_URL() function.')

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
                        df = pd.read_json(io.StringIO(web_content.decode('utf-8', errors='replace')))

                    case 'application/xml' | 'text/xml':
                        df = pd.read_xml(io.StringIO(web_content.decode('utf-8', errors='replace')))

                    case _:
                        print("Default case url")
        
        except HTTPError as e:
            logger.error(f'HTTP Error: {e.code} - {e.reason}')
        except URLError as e:
            logger.error(f'URL Error: {e.reason}')
        except Exception as e:
            logger.error(f'Unexpected error: {e}')

        finally:
            return df


    elif is_valid_path(source_path):

        filetype = Path(source_path).suffix.lower()

        source = source_path

        # This may not be the shortest or quickest way to do this.
        # I am prioritizing readability.
        match filetype:
        
            case ".csv":
                try:
                    df = pd.read_csv(source)
                except FileNotFoundError as e:
                    print(f"File not found: {e}")

            case ".json":
                df = pd.read_json(source)

            case ".xml":
                try:
                    df = pd.read_xml(source)
                except FileNotFoundError:
                    print(f"File not found: {e}")

            case _:
                print(f'Default case, nothing happens.')
                logger.debug(f'{source} default')
                pass

    return df

def is_valid_url(source: str) -> bool:
    parsed = urlparse(source)
    if parsed.scheme in ('http', 'https', 'ftp') and parsed.netloc:
        return True
    return False


def is_valid_path(path: str) -> bool:
    try:
        path = Path(path)
        return path.exists() and path.is_file()
    
    except Exception:
        return False
    