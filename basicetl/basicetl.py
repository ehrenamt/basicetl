"""Main class for the basicetl package."""

from pathlib import Path
from datetime import datetime

# project imports
from .submodules import extract as ex
from .submodules.transform import configured_transform
from .submodules.controllers import SubserviceController


DEFAULT_CONFIG_PATH = Path("basicetl-config.yml")


class BasicETL:
    """
    A class for configuring and executing simple ETL tasks.
    
    BasicETL acts as a top-level facade or API to interact only with
    the in-memory data and not the underlying functions.
    """


    def __init__(self, jointype="natural", output_type="df", destination=""):
        self.sources = []
        self.jointype = jointype
        self.extracted = []
        self.transformed = []
        self.output_type = output_type
        self.destination = destination
        self.controller = SubserviceController()


    def extract(self, sources: list = None):
        """
        Extracts sources and loads into memory.
        Extraction logic and configuration is handled by the subservice controller.
        """

        if sources is None:
            sources = self.sources

        for source in sources:

            ex.get_source_type(source)

            # if source not in ?
            self.extracted.append(ex.extract_based_on_source(source))


    def transform(self):

        # In addition to basic transformations such as merging, joining, and cleaning,

        # We'll add the functionality to pass in functions that perform normalization.
        # Likely needing a control object or something similar, and function templates,
        # in order to quickly add and remove transformation steps.

        # This of course depends on the schema, which is not known beforehand,
        # which is why this flexibility is needed.

        sources = self.extracted

        configured_transform(sources=sources)


    def load(self, functions = None):
        # if (functions == []):
        #     submodules.load.save_local()
        return


    def etl(self, sources: list):
        """Executes the entire ETL process."""

        time_start = datetime.now()

        print('Beginning ETL processes...')

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
