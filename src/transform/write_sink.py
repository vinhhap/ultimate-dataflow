from os import pipe
import apache_beam as beam
from helper.argparser import RunParam

class PCollToSink(beam.PTransform):
    def __init__(self, source_sink: RunParam, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.source_sink = source_sink

    def expand(self, pcoll):
        transform = None
        sink_type = self.source_sink.sink_type
        name = self.source_sink.name
        sink = self.source_sink.sink
        if 'bigquery' in sink_type:
            transform = (
                pcoll | f'Write {name} to BigQuery' >> beam.io.WriteToBigQuery(
                    table=sink,
                    schema='SCHEMA_AUTODETECT',
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                )
            )
        return transform