
# extract.py - handles pulling from various sources and converts to a standard format

import logging
import os
import pandas as pd
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import urlretrieve

logger = logging.getLogger(__name__)

def extract_json(source):
    script_dir = os.path.dirname(os.path.abspath(__file__)) 
    full_json_path = os.path.join(script_dir, source) 
    json_df = pd.read_json(full_json_path)

    return json_df


def extract_csv(source):
    script_dir = os.path.dirname(os.path.abspath(__file__)) 
    full_csv_path = os.path.join(script_dir, source) 
    csv_df = pd.read_csv(full_csv_path)

    return csv_df


def extract_xml(source):
    script_dir = os.path.dirname(os.path.abspath(__file__)) 
    full_xml_path = os.path.join(script_dir, source) 
    xml_df = pd.read_xml(full_xml_path)

    return xml_df


def extract_based_on_source(source: str, output='df'):

    print(f'source points to {source}')


    if is_valid_url(source):

        print('is url')

        source, header = urlretrieve(source)

        print(f'source points to {source}')
    

    if is_valid_path(source):

        logger.info(f'Source points to {source}')

        filetype = Path(source).suffix.lower()

        logger.info(f'Filetype {source}')

        df = None

        match filetype:

            case ".csv":
                df = extract_csv(source)

            case ".json":
                df = extract_json(source)

            case ".xml":
                df = extract_xml(source)

            case _:
                print(f'Default case, nothing happens.')

        return df

    return None


def is_valid_url(source: str) -> bool:
    parsed = urlparse(source)
    if parsed.scheme in ('http', 'https', 'ftp') and parsed.netloc:
        return True
    return False


def is_valid_path(source: str) -> bool:
    try:
        Path(source)
        return True
    except Exception:
        return False

# extract_based_on_source('file.xml')
# extract_based_on_source('https://people.sc.fsu.edu/~jburkardt/data/csv/cities.csv ')
