
from datetime import datetime
from modules.extract import extract_based_on_source
from modules.transform import configured_transform
import modules.load



class BasicETL:


    def __init__(self, jointype="natural", output_type="df", destination=""):
        self.jointype = jointype
        self.extracted = []
        self.transformed = []
        self.output_type = output_type
        self.destination = destination
        return
    

    def extract(self, sources: list):
        for source in sources:
            self.extracted.append(extract_based_on_source(source))
    

    def transform(self):

        # In addition to basic transformations such as merging, joining, and cleaning,

        # We'll add the functionality to pass in functions that perform normalization. Likely needing a control object or something similar, and function templates, in order to quickly add and remove transformation steps.

        # This of course depends on the schema, which is not known beforehand, which is why this flexibility is needed.

        sources = self.extracted

        configured_transform(sources=sources)

    def load(self, functions = []):
        if (functions == []):
            modules.load.save_local()
            
        
    def etl(self, sources: list):
        """Executes the entire ETL process."""

        # potentially, we could use a state member variable to compare timings?

        time_start = datetime.now()

        self.extract(sources)

        self.transform()

        # same thing. If destination = "" then simply return the df. Or collection of dfs.
        self.load()

        time_end = datetime.now()
        time_elapsed = time_end - time_start

        total_seconds = int(time_elapsed.total_seconds())
        minutes = total_seconds // 60
        seconds = total_seconds % 60

        print(f'Time elapsed (mm:ss): {minutes:02d}:{seconds:02d}')
