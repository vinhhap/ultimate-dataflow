import apache_beam as beam
from helper.argparser import RunParam
import logging
from source_sink import ConnectorMapper, ConnectorType
from transform.read import *

class SourceToPCollWithInit(beam.PTransform):
    def __init__(self, source_sink: RunParam, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.source_sink = source_sink
        
    def expand(self, pcoll):
        # Extract info from PCollection
        name = self.source_sink.name
        source_type = self.source_sink.source_type
        connector_type_str = source_type.split('|')[0]

        connector_mapper = ConnectorType(**ConnectorMapper().get_connector_type(connector_type_str))
        # RDBMS sources
        if connector_mapper.type == 'RDBMS':
            transform = (
                pcoll
                | f"Read {name} from {connector_type_str.upper()}" >> beam.ParDo(connector_mapper.connector_in())
                | f"Reshuffle {name} chunk" >> beam.Reshuffle()
                | f"Convert {name} to dict" >> beam.ParDo(ConvertSourceToDict())
            )
            return transform

        # File source
        if connector_type_str == 'csv':
            transform = (
                pcoll
                | f"List {name} files" >> beam.ParDo(ListFiles())
                | f"Reshuffle {name} files" >> beam.Reshuffle()
                | f"Read {name} from {connector_type_str.upper()}" >> beam.ParDo(connector_mapper.connector_in(self.source_sink))
                | f"Convert {name} to dict" >> beam.ParDo(ConvertSourceToDict())
            )
            return transform

        # TODO: Add other sources

class SourceToPColl:
    def __init__(self, source_sink: RunParam) -> None:
        self.source_sink = source_sink

    def expand(self) -> beam.PTransform:
        # Extract info from PCollection
        source = self.source_sink.source
        source_type = self.source_sink.source_type
        connector_type_str = source_type.split('|')[0]
        pipeline_options = self.source_sink.pipeline_options
        read_options = pipeline_options.get('read_options', {})

        connector_mapper = ConnectorType(**ConnectorMapper().get_connector_type(connector_type_str))
        transform = beam.PTransform()
        # BigQuery source
        if connector_type_str == 'bigquery':
            input_sql = {"table": source} if source_type.split('|')[1] == 'from_table' else {"query": source}
            transform = connector_mapper.connector_in(method='DIRECT_READ', **input_sql, **read_options)
        
        return transform
        
        