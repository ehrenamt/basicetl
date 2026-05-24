"""
-------------------------------------------------------------------------------
Copyright (C) 2026 Hussein Adi

This program is free software, and may be used and distributed under the terms
of the included Hippocratic License. You should have received a copy of this
license. If not, see https://github.com/ehrenamt/basicetl/blob/main/LICENSE.md
and the Hippocratic License at https://firstdonoharm.dev/

-------------------------------------------------------------------------------
Filename: basicetl.py

Main class for the basicetl package.
-------------------------------------------------------------------------------
"""

# Python standard library imports
from datetime import datetime
import os
from pathlib import Path


# Project imports
from .submodules import extract as ex
from .submodules.transform import configured_transform, concatenate_sources
from .submodules import load as ld
from .etl_options import ExtractOptions, LoadOptions


# DEFAULT_CONFIG_PATH = Path("basicetl-config.yml")


class BasicETL:
    """
    A class for configuring and executing simple ETL tasks.

    BasicETL acts as a top-level facade or API to interact only with
    the in-memory data and not the underlying functions.
    """

    def __init__(self):
        self.sources: list[str] = []
        self.extracted: list = []
        self.transformed: list = []
        self.transformed_sources: list = []
        self.caller_path: str = Path(os.getcwd()).resolve()

    def add_source(self, source: str):
        self.sources.append(source)

    def add_sources(self, *args):
        self.sources.extend(args)

    def extract(self, sources: list = None):
        """
        Extracts sources and loads into memory.
        """

        if sources is None:
            sources = self.sources

        for source in sources:

            ex.get_source_type(self.caller_path, source)

            extracted_data = ex.extract_based_on_source(
                self.caller_path,
                source,
                options=ExtractOptions()
            )

            if extracted_data is not None:
                # Stores the in-memory df in the extracted list.
                # Likely high memory usage here.
                self.extracted.append(extracted_data)

        return

    def transform(
        self,
        e_sources: list = None,
        t_options = None,
        *customized_transformations
    ) -> None:
        """
        Transforms the extracted sources in a 3-step process.
        Basic, configured transformations are executed, followed by custom
        transformations. Lastly, transformations that are dependent on previous
        transformations are executed.
        """

        if ((t_options is None) and (not customized_transformations)):
            print(
                'No config passed into transformation stage, '
                'proceeding with default transformations.'
            )

        if e_sources is None:
            e_sources = self.extracted

        for source in e_sources:
            interim_t_source = configured_transform(source, t_options)

            for transformation in customized_transformations:
                if callable(transformation):
                    try:
                        interim_t_source = transformation(interim_t_source)

                    except Exception as e:
                        print(
                            f'Error: {e}.'
                            ' (Possible non-callable transformation'
                            ' or invalid function signature.)'
                        )

            if interim_t_source is not None:
                if not interim_t_source.empty:
                    self.transformed_sources.append(interim_t_source)

        if t_options.concatenate:
            self.transformed_sources = concatenate_sources(
                self.transformed_sources
            )

        return

    def load(self, l_options: LoadOptions = None):

        if l_options is None:
            l_options = LoadOptions()

        ld.load(self.transformed_sources, l_options)

    def etl(self, sources: list):
        """Executes the entire ETL process in-order."""

        time_start = datetime.now()

        print('Beginning ETL processes...')

        if sources is None:
            sources = self.sources

        self.extract(sources)

        self.transform()

        self.load()

        time_end = datetime.now()
        time_elapsed = time_end - time_start

        total_seconds = int(time_elapsed.total_seconds())
        minutes = total_seconds // 60
        seconds = total_seconds % 60

        print('ETL processes complete.')
        print(f'Time elapsed (mm:ss): {minutes:02d}:{seconds:02d}')
