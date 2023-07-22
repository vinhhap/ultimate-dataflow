import apache_beam as beam
from helper.argparser import RunParam
from helper.secret import get_secret
from sqlalchemy.sql import text
import logging
from source_sink.rdbms.rdbms_connector import *
from pandas import DataFrame

class ReadRDBMS(beam.DoFn):
    def process(self, context: RunParam):
        # Extract info from context
        name = context.name
        source = context.source
        source_type = context.source_type
        connector_type_str = source_type.split('|')[0]
        source_connection_secret = context.source_connection_secret
        pipeline_options = context.pipeline_options

        # Create connector
        logging.info(f"Create source connector for {connector_type_str.upper()}")
        conn_info = RDBMSConnectionInfo(**get_secret(source_connection_secret))
        connector = RDBMSConnectionMapper[connector_type_str](conn_info)

        # Set default variables
        chunksize = pipeline_options.get('chunksize', 50000)

        # Build SQL based on query or table name
        input_sql = ''
        if 'from_query' in source_type:
            input_sql = text(source)
        elif 'from_table' in source_type:
            input_sql = source

        # Read and return result as pd.DataFrame
        logging.info(f"Reading data for {name} using in chunks of {chunksize}")
        return connector.read_from_sql(name=name, sql=input_sql, chunksize=chunksize)

class ConvertSourceToDict(beam.DoFn):
    def process(self, df: DataFrame):
        for row in df.to_dict(orient='records'):
            yield row