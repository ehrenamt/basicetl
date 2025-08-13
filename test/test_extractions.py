import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from basicetl import BasicETL

REMOTE_SOURCE_1 = 'https://people.sc.fsu.edu/~jburkardt/data/csv/cities.csv'

LOCAL_SOURCE_A1 = 'data/datafile_schema1_1.json'
LOCAL_SOURCE_A2 = 'data/datafile_schema1_2.json'
LOCAL_SOURCE_A3 = 'data/datafile_schema1_3.json'

local_a = [LOCAL_SOURCE_A1, LOCAL_SOURCE_A2, LOCAL_SOURCE_A3]


def test_extractions():
    basicetl = BasicETL()

    # TODO: when passing in a file without an extension, this passes the valid check
    # and ends up causing a preventable AttributeError nonetype in concatenate sources
    basicetl.etl(local_a)

