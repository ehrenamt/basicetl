# Sample placeholder - not meant to work

from basicetl import BasicETL
# from basicetl.controllers import transform_options

LOCAL_SOURCE_A1 = '../data/datafile_schema1_1.json'
LOCAL_SOURCE_A2 = '../data/datafile_schema1_2.json'
LOCAL_SOURCE_A3 = '../data/datafile_schema1_3.json'

local_a = [LOCAL_SOURCE_A1, LOCAL_SOURCE_A2, LOCAL_SOURCE_A3]

basicetl = BasicETL()
basicetl.extract(local_a)


# t_options = transform_options.TransformOptions()

# basicetl.transform(t_options=t_options)
