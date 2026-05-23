"""etl_types.py"""

# Python standard library imports
from enum import Enum, auto


class SourceType(Enum):
    """Represents source type during extraction."""

    URL = auto()
    FILE = auto()
    SQLCONN = auto()


class DataType(Enum):
    """Represents data format/type during extraction."""

    CSV = auto()
    JSON = auto()
    XML = auto()
