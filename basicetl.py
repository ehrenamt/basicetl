
from datetime import datetime
from modules.extract import extract_based_on_source


class BasicETL:
    def __init__(self, jointype="natural", output="df", destination=""):
        self.jointype = jointype
        self.extracted = []
        self.output = output
        self.destination = destination
        return
    

    def extract(self, sources: list):
        for source in sources:
            self.extracted.append(extract_based_on_source(source))
        return
    

    def transform():

        # In addition to basic transformations such as merging, joining, and cleaning,

        # We'll add the functionality to pass in functions that perform normalization. Likely needing a control object or something similar, and function templates, in order to quickly add and remove transformation steps.

        # This of course depends on the schema, which is not known beforehand, which is why this flexibility is needed.

        
        return
        

    def etl(self, sources: list):

        time_start = datetime.now()

        self.extract(sources)

        self.transform()

        # same thing. If destination = "" then simply return the df. Or collection of dfs. Can skip load
        self.load()

        return
