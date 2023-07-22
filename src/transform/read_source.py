import apache_beam as beam
from helper.argparser import RunParam
import logging
from source_sink.rdbms.rdbms_connector import *
from source_sink.connector_mapper import ConnectorMapper, ConnectorType
from transform.read.read_rdbms import *

class SourceToPColl(beam.PTransform):
    def __init__(self, source_sink: RunParam, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.source_sink = source_sink
        
    def expand(self, pcoll):
        # Extract info from PCollection
        name = self.source_sink.name
        source_type = self.source_sink.source_type
        connector_type_str = source_type.split('|')[0]
        pipeline_options = self.source_sink.pipeline_options
        read_options = pipeline_options.get('read_options', {})

        # RDBMS sources
        connector_mapper = ConnectorType(**ConnectorMapper().get_connector_type(connector_type_str))
        if connector_mapper.type == 'RDBMS':
            transform = (
                pcoll
                | f"Read {name} from {connector_type_str.upper()}" >> beam.ParDo(ReadRDBMS())
                | f"Reshuffle {name} chunk" >> beam.Reshuffle()
                | f"Convert {name} to dict" >> beam.ParDo(ConvertSourceToDict())
            )
            return transform
        
        # TODO: Add other sources

         