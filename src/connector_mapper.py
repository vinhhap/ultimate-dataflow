import apache_beam as beam
import typing
from transform.read import *
from transform.write import *

class ConnectorType(typing.NamedTuple):
    connector_in: typing.Any
    connector_out: typing.Any
    need_initialization: bool
    type: str

class ConnectorMapper:
    CONNECTOR_TYPE_MAPPER = {
        'oracle': {
            "connector_in": ReadRDBMS,
            "connector_out": WriteRDBMS,
            "need_initialization": True,
            "type": "RDBMS"
        },
        'mysql': {
            "connector_in": ReadRDBMS,
            "connector_out": WriteRDBMS,
            "need_initialization": True,
            "type": "RDBMS"
        },
        'postgres': {
            "connector_in": ReadRDBMS,
            "connector_out": WriteRDBMS,
            "need_initialization": True,
            "type": "RDBMS"
        },
        'avro': {
            "connector_in": beam.io.ReadFromAvro,
            "connector_out": beam.io.WriteToAvro,
            "need_initialization": False,
            "type": "File"
        },
        'parquet': {
            "connector_in": beam.io.ReadFromParquet,
            "connector_out": beam.io.WriteToParquet,
            "need_initialization": False,
            "type": "File"
        },
        'csv': {
            "connector_in": ReadCsv,
            "connector_out": WriteCsv,
            "need_initialization": True,
            "type": "File"
        },
        'bigquery': {
            "connector_in": beam.io.ReadFromBigQuery,
            "connector_out": beam.io.WriteToBigQuery,
            "need_initialization": False,
            "type": "BigQuery"
        }
    }

    @classmethod
    def get_connector_type(cls, conn_type_str: str) -> dict:
        return cls.CONNECTOR_TYPE_MAPPER.get(conn_type_str, {})