"""
-------------------------------------------------------------------------------
Copyright (C) 2026 Hussein Adi

This program is free software, and may be used and distributed under the terms
of the included Hippocratic License. You should have received a copy of this
license. If not, see https://github.com/ehrenamt/basicetl/blob/main/LICENSE.md
and the Hippocratic License at https://firstdonoharm.dev/

-------------------------------------------------------------------------------
Filename: transform.py

Implements and abstracts logic handling default behaviour for missing values,
join types, and filters.
-------------------------------------------------------------------------------
"""


# Python standard library imports
import copy

# PyPI imports
import pandas as pd


def configured_transform(
    source: pd.DataFrame,
    t_options = None
) -> pd.DataFrame:
    """
    Transforms extracted sources based on the supplied configuration.

    If no configuration is found, it will perform some basic transformations.
    """

    if source is None:
        return None

    copied_source = copy.deepcopy(source)

    if t_options.dropna:
        copied_source.dropna(inplace=True)

    if t_options.drop is not None:
        column = t_options.drop
        copied_source.drop(column, axis=1, inplace=True)

    return copied_source


def concatenate_sources(t_sources: list) -> list:
    """
    Accepts a list of sources and returns a list of sources made by
    concatenating all sources with an identical schema.
    """

    # return variable
    cumulative_dfs = []

    # the weird solution below is because pd.Dateframes and pd.Series cannot
    # be hashed, and therefore cannot be used in a set.
    # My solution to this is to use a tuple, which is hashable
    unique_dtypes = {}

    # this is will need refactoring
    for source in t_sources:

        dtypes = source.dtypes

        dtypes_key = tuple(dtypes.astype(str).items())

        # print(f'{dtypes_key}')

        if dtypes_key in unique_dtypes:
            unique_dtypes[dtypes_key].append(source)

        else:
            unique_dtypes[dtypes_key] = [source]

    # start accumulating dfs with the same schema into a single df
    for _, value in unique_dtypes.items():
        cumulative_df = value[0]

        for df in value[1:]:

            cumulative_df = pd.concat([cumulative_df, df], ignore_index=True)

        cumulative_dfs.append(cumulative_df)

    return cumulative_dfs


# pylint: disable=missing-docstring


def check_schema_match(df_1: pd.DataFrame, df_2: pd.DataFrame) -> bool:
    return df_1.dtypes.equals(df_2.dtypes)


def remove_invalid_rows(source: pd.core.frame.DataFrame):
    return source.dropna(axis=0, how="any")


# not sure how efficient this is...is converting them to a string faster?
def dtypes_are_equal(df1: pd.Series, df2: pd.Series) -> bool:
    return df1.sort_index().to_dict() == df2.sort_index().to_dict()

