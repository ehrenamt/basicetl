"""
transform.py

Implements and abstracts logic handling default behaviour for missing values,
join types, and filters.

"""

import copy
from typing import Callable
import pandas as pd


def configured_transform(sources, *customized_transformations: Callable):
    """
    Transforms extracted sources based on the supplied configuration.

    It accepts configuration either py supplying an argument or via a basicetl-config.yml.
    If no configuration is found, it will perform some basic transformations.
    """

    # TODO: placeholder - needs major rework

    transformed_sources = copy.deepcopy(sources)
    for i, func in customized_transformations:
        for source in sources:
            try:
                transformed_sources[i] = func(source)


            # TODO fix with proper logger, refactor logger here fro subservices
            except Exception:
                pass

    concatenate_sources(sources)

    return transformed_sources


def concatenate_sources(sources: list) -> list:
    """
    Accepts a list of sources and returns a list of sources made by
    concatenating sources with the same schema.
    """

    # return variable
    cumulative_dfs = []

    source_dict = {}

    # dtypes cannot be hashed.
    # instead, we use a list and assign an id manually in the dictionary
    unique_dtypes = []
    next_id = 0

    for source in sources:
        dtype_key = source.dtypes
        if dtype_key not in unique_dtypes:
            source_dict[next_id] = [source]

            unique_dtypes.append(dtype_key)

            next_id +=1

        else:
            # ?????????????
            source_dict.key(source).append(source)

    for _, dfs in unique_dtypes.items():
        cumulative_df: pd.core.frame.DataFrame = dfs[0]

        for df in dfs[1:]:
            if (check_schema_match(cumulative_df, source)):
                pd.concat([cumulative_df, df])

        cumulative_df = cumulative_df.drop_duplicates()

        cumulative_dfs.append(cumulative_df)

    return cumulative_dfs


# basic helpers
# pylint: disable=missing-docstring


def check_schema_match(df_1: pd.core.frame.DataFrame, df_2: pd.core.frame.DataFrame) -> bool:
    return df_1.dtypes.equals(df_2.dtypes)


def remove_invalid_rows(source: pd.core.frame.DataFrame):
    return source.dropna(axis=0, how="any")
