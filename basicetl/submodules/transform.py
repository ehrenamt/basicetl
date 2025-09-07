#
# transform.py
#
# This handles default behaviour for missing values, join types, filters, and other transformations
#

import pandas as pd


def configured_transform(sources, functions: list = []):
    concatenate_sources(sources)
    return


def check_schema_match(df_1: pd.core.frame.DataFrame, df_2: pd.core.frame.DataFrame) -> bool:
    return df_1.dtypes.equals(df_2.dtypes)


def concatenate_sources(sources: list) -> list:

    unique_dtypes = dict()
    cumulative_dfs = []

    for source in sources:
        dtype_key = source.dtypes
        if dtype_key not in unique_dtypes:
            unique_dtypes[dtype_key] = [source]
        else:
            unique_dtypes[dtype_key].append(source)

    for _, dfs in unique_dtypes.items():
        cumulative_df: pd.core.frame.DataFrame = dfs[0]

        for df in dfs[1:]:
            if (check_schema_match(cumulative_df, source)):
                pd.concat([cumulative_df, df])

        cumulative_df = cumulative_df.drop_duplicates()
        
        cumulative_dfs.append(cumulative_df)


    return cumulative_dfs

 