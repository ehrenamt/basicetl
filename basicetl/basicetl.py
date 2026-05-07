"""Main class for the basicetl package."""

from pathlib import Path
from datetime import datetime

# project imports
from .controllers.submodules import extract as ex
from .controllers.submodules.transform import configured_transform
from .controllers.submodule_controller import SubserviceController


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

    
    def add_sources(self, *args):
        self.sources.extend(args)


    def extract(self, sources: list = None):
        """
        Extracts sources and loads into memory.
        Extraction logic and configuration is handled by the subservice controller.
        """

        if sources is None:
            sources = self.sources

        for source in sources:

            ex.get_source_type(source)

            extracted_data = ex.extract_based_on_source(source)

            if extracted_data is not None:
                # stores the in-memory df in the extracted list, likely high memory usage here.
                self.extracted.append(extracted_data)


    def transform(self, extracted_sources: list = None, t_options = None, *customized_transformations):

        if (extracted_sources is None) and (t_options is None) and (customized_transformations == ()):
            print(f'No arguments passed into transformation stage, proceeding with default transformations.')

        if extracted_sources is None:
            extracted_sources = self.extracted

        for source in transformed_sources:
            interim_transformation_source = configured_transform(source, t_options)

            for transformation in customized_transformations:
                if callable(transformation):
                    try:
                        interim_transformation_source = transformation(interim_transformation_source)

                    except Exception as e:
                        print(f'Error: {e}. (Possible non-callable transformation or invalid function signature.)')

            if interim_transformation_source is not None:
                transformed_sources.append(interim_transformation_source)


    def load(self, *functions):
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

    def etla(self, sources: list):
        # tentative. etl + analysis
        return
