
from datetime import datetime

class SimpleETL:
    def __init__(self, jointype="natural", output="df", destination=""):
        self.jointype = jointype
        self.extracted = []
        self.output = output
        self.destination = destination
        return
    
    def extract(self, sources: list):
        return
    
    def add_df_to_ndarray(self):
        return

    def etl(self, sources: list):

        time_start = datetime.now()

        self.extract(sources)

        self.transform()

        # same thing. If destination = "" then simply return the df. Or collection of dfs. Can skip load
        self.load()

        return
