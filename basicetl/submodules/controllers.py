"""Implementation for the SubserviceController class."""


from dataclasses import dataclass

# third-party imports
import yaml

# project imports (subservices)
import extract
import load


@dataclass(frozen=True)
class ExtractOptions:
    """Configuration class for the extraction subservice."""

    save_to_disk: bool = False
    output_type: str = "df"
    chunk_size: int = 8192
    sources: list = None


@dataclass(frozen=True)
class TransformOptions:
    """Configuration class for the transformation subservice."""

    in_place: bool = False
    join_type: str = "inner"


@dataclass(frozen=True)
class LoadOptions:
    """Configuration class for the loading subservice."""

    save_to_disk: bool = False
    dirname: str = "unnamed_etl_output"


class SubserviceController:
    """
    The SubserviceController class handles errors and enforces configurations.
    """


    def __init__(self):
        self.jointype = True # TODO placeholder, will replace


    def run_extract(self):
        """
        Resolves extraction config settings and then passes them to the extraction subservice.
        """

        self._resolve_config()


    def run_transform(self):
        self._resolve_config()


    # TODO manage SQL connections for efficiency.
    def run_load(self):
        self._resolve_config()


    def _resolve_config(self):
        # read path to config
        # check if set
        # resolve, set and end
        return


    # TODO float errors up here and format messages for each _run function
