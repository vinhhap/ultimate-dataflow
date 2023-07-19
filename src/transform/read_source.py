import apache_beam as beam
from helper.argparser import RunParam
from source_sink.rdbms.rdbms_connector import OracleConnector
from helper.secret import get_secret
from sqlalchemy.sql import text
from sqlalchemy.sql.elements import TextClause
import logging
import pandas as pd
from pandas import DataFrame
from typing import Any, Iterator, Union
from source_sink.rdbms.rdbms_connector import ConnectionInfo

class ChooseSourceConnection(beam.DoFn):
    def process(self, context: RunParam):
        connector = None
        if 'oracle' in context.source_type:
            conn_info = ConnectionInfo(**get_secret(context.source_connection_secret))
            connector = OracleConnector(conn_info)
        
        # TODO: Add other connection types here

        yield {
            'connector': connector,
            'run_param': context
        }

class ReadSource(beam.DoFn):
    def read_from_sql(self, 
                     name: str, 
                     connector: Any, 
                     sql: Union[TextClause, str],
                     chunksize: int) -> Iterator[DataFrame]:
        conn = connector.get_database_connection()
        try:
            logging.info(f"Reading data for {name} using in chunks of {chunksize}")
            for chunk in pd.read_sql(sql, con=conn, chunksize=chunksize):
                yield chunk
        except Exception as e:
            raise Exception(f"Error reading data from source {name} - {e}")
        finally:
            logging.info(f"Closing database connection")
            connector.close_engine()

    def process(self, context: dict):
        run_param: RunParam = context['run_param']
        connector = context['connector']
        pipeline_options = run_param.pipeline_options
        chunksize = pipeline_options['chunksize'] if 'chunksize' in pipeline_options.keys() else 50000

        if 'from_query' in run_param.source_type:
            query = text(run_param.source)
            return self.read_from_sql(run_param.name, connector, query, chunksize)
        elif 'from_table' in run_param.source_type:
            source_table = run_param.source
            return self.read_from_sql(run_param.name, connector, source_table, chunksize)

class ConvertSourceToDict(beam.DoFn):
    def process(self, df: DataFrame):
        for row in df.to_dict(orient='records'):
            yield row

class SourceToPColl(beam.PTransform):
    def expand(self, pcoll):
        return (
            pcoll
            | "Choose connection" >> beam.ParDo(ChooseSourceConnection())
            | "Read source" >> beam.ParDo(ReadSource())
            | "Reshuffle source chunk" >> beam.Reshuffle()
            | "Convert to dict" >> beam.ParDo(ConvertSourceToDict())
        )