import apache_beam as beam
import typing
from transform.read import *
from transform.write import *

class ConnectorType(typing.NamedTuple):
    connector_in: typing.Any
    connector_out: typing.Any
    type: str

class ConnectorMapper:
    CONNECTOR_TYPE_MAPPER = {
        'oracle': {
            "connector_in": ReadRDBMS,
            "connector_out": WriteRDBMS,
            "type": "RDBMS"
        },
        'mysql': {
            "connector_in": ReadRDBMS,
            "connector_out": WriteRDBMS,
            "type": "RDBMS"
        },
        'postgres': {
            "connector_in": ReadRDBMS,
            "connector_out": WriteRDBMS,
            "type": "RDBMS"
        },
        'avro': {
            "connector_in": beam.io.ReadFromAvro,
            "connector_out": beam.io.WriteToAvro,
            "type": "File"
        },
        'parquet': {
            "connector_in": beam.io.ReadFromParquet,
            "connector_out": beam.io.WriteToParquet,
            "type": "File"
        },
        'csv': {
            "connector_in": beam.io.ReadFromCsv,
            "connector_out": WriteCsv,
            "type": "File"
        },
        'bigquery': {
            "connector_in": None,
            "connector_out": beam.io.WriteToBigQuery,
            "type": "BigQuery"
        }
    }

    @classmethod
    def get_connector_type(cls, conn_type_str: str) -> dict:
        return cls.CONNECTOR_TYPE_MAPPER.get(conn_type_str, {})