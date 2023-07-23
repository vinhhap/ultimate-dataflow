import apache_beam as beam
import logging
from helper.argparser import RunParam
from source_sink import ConnectorMapper, ConnectorType

class PCollToSink(beam.PTransform):
    def __init__(self, source_sink: RunParam, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.source_sink = source_sink

    def expand(self, pcoll):
        # Extract info from class attribute
        name = self.source_sink.name
        sink = self.source_sink.sink
        sink_type = self.source_sink.sink_type

        # RDBMS sinks
        connector_mapper = ConnectorType(**ConnectorMapper().get_connector_type(sink_type))
        if connector_mapper.type == 'RDBMS':
            transform = (
                pcoll | f'Write {name} to {sink_type.upper()}' >> beam.ParDo(connector_mapper.connector_out(source_sink=self.source_sink))
            )
            return transform

        # BigQuery sink
        if connector_mapper.type == 'BigQuery':
            transform = (
                pcoll | f'Write {name} to {sink_type.upper()}' >> connector_mapper.connector_out(
                    table=sink,
                    schema='SCHEMA_AUTODETECT',
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                )
            )
            return transform

        # File sinks
        if sink_type == 'csv':
            transform = (
                pcoll | f'Write {name} to {sink_type.upper()}' >> beam.ParDo(connector_mapper.connector_out(source_sink=self.source_sink))
            )
            return transform

        # TODO: Add other sinks