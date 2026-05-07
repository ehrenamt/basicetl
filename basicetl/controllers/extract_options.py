"""Implementation of the ExtractOptions class."""

from dataclasses import dataclass


@dataclass(frozen=True)
class ExtractOptions:
    """Configuration class for the extraction submodule."""

    output_type: str = "df"
    chunk_size: int = 8192
    sources: list = None
