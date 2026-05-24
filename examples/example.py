""""
-------------------------------------------------------------------------------
filename: example.py

Sample usage of the BasicETL class.
-------------------------------------------------------------------------------
"""

# pylint: disable=missing-docstring

from basicetl import BasicETL, LoadOptions, TransformOptions, DataType


LOCAL_SOURCE_A1 = './data/datafile_schema1_1.json'
LOCAL_SOURCE_A2 = './data/datafile_schema1_2.json'
LOCAL_SOURCE_B1 = './data/datafile_schema2_1.csv'
LOCAL_SOURCE_B2 = './data/datafile_schema2_2.csv'

local_sources = [
    LOCAL_SOURCE_A1,
    LOCAL_SOURCE_A2,
    LOCAL_SOURCE_B1,
    LOCAL_SOURCE_B2
]

etl = BasicETL()
etl.extract(local_sources)

print("---------------------\n- Extracted sources:\n")

for edf in etl.extracted:
    print(edf.head())
    
t_options = TransformOptions(drop="subregion")

# defining a custom transformation
def population_to_millions(target_df):
    target_df['population'] = target_df['population'].map(lambda x : x / 1000000)
    return target_df

etl.transform(None, t_options, population_to_millions)

print("---------------------\n- Transformed sources:")

for tdf in etl.transformed_sources:
    print(tdf.head())

print("---------")

etl.load(LoadOptions(save_to_disk=True, filetype=DataType.CSV))
