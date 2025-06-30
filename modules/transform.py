#
# transform.py
#
# This handles default behaviour for missing values, join types, filters, and other transformations
#

import pandas as pd

def check_schema_match(df_1: pd.core.frame.DataFrame, df_2: pd.core.frame.DataFrame) -> bool:
    return df_1.dtypes.equals(df_2.dtypes)

def merge_sources(sources: list) -> list:
    for source in sources:
        pass
    return []

 