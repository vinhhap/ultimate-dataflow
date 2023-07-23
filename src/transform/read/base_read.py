import apache_beam as beam
import pandas as pd

class ConvertSourceToDict(beam.DoFn):
    def process(self, df: pd.DataFrame):
        for row in df.to_dict(orient='records'):
            yield row