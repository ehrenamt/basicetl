"""Implementation of the LoadOptions class."""

from dataclasses import dataclass

@dataclass(frozen=True)
class LoadOptions:
    """Configuration class for the loading subservice."""

    save_to_disk: bool = False
    dirname: str = "unnamed_etl_output"
