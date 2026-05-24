"""
-------------------------------------------------------------------------------
Copyright (C) 2026 Hussein Adi

This program is free software, and may be used and distributed under the terms
of the included Hippocratic License. You should have received a copy of this
license. If not, see https://github.com/ehrenamt/basicetl/blob/main/LICENSE.md
and the Hippocratic License at https://firstdonoharm.dev/

-------------------------------------------------------------------------------
Filename: etl_options.py

Provides classes to use for runtime configuration of the ETL process.
-------------------------------------------------------------------------------
"""


# Python standard library imports
from dataclasses import dataclass

# Project imports
from .etl_types import DataType


@dataclass(frozen=True)
class ExtractOptions:
    """Configuration class for the extraction subservice."""

    output_type: str = "df"     # representation in memory
    chunk_size: int = 8192      # only pertinent for loading large files


@dataclass(frozen=True)
class LoadOptions:
    """Configuration class for the loading subservice."""

    save_to_disk: bool = True
    filetype: DataType = DataType.CSV
    destination_dirname: str = "etl_output/"
    filename: str = "unnamed_etl_output_file"


@dataclass(frozen=True)
class TransformOptions:
    """Configuration class for the transformation subservice."""

    in_place: bool = False
    join_type: str = "inner"    # alt: "outer"
    dropna: bool = True
    drop: list = None           # names of columns
    concatenate: bool = True
