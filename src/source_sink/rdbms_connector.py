import sqlalchemy
import pandas as pd
from typing import Any, Dict, Union, NamedTuple, Iterator
from sqlalchemy.engine.base import Connection
from sqlalchemy.engine import Engine
from sqlalchemy.sql.elements import TextClause

class RDBMSConnectionInfo(NamedTuple):
    dialect: str
    host: str
    port: Union[str, int]
    database: str
    username: str
    password: str
    engine_options: Dict[str, Any]

class BaseRDMSConnector:
    def __init__(self, conn_info: RDBMSConnectionInfo, server_side_cursors: bool=True, stream_results: bool=True) -> None:
        self.conn_info = conn_info
        self.conn_string = self._build_conn_string()
        self.server_side_cursors = server_side_cursors
        self.stream_results = stream_results
        self.engine = None
        
    def _build_conn_string(self) -> str:
        return f"{self.conn_info.dialect}://{self.conn_info.username}:{self.conn_info.password}@{self.conn_info.host}:{self.conn_info.port}/{self.conn_info.database}"
    
    def create_engine(self) -> Engine:
        return sqlalchemy.create_engine(
            self.conn_string,
            **self.conn_info.engine_options
        )
        
    def close_engine(self) -> None:
        if self.engine:
            self.engine.dispose()
        
    def get_database_connection(self) -> Connection:
        if self.engine is None:
            self.engine = self.create_engine()
        self.engine.dialect.server_side_cursors = self.server_side_cursors
        self.engine.execution_options(stream_results=self.stream_results)
        return self.engine.connect()

    def read_from_sql(self, name: str, sql: Union[TextClause, str], chunksize: int=50000, read_options: dict={}) -> Iterator[pd.DataFrame]:
        conn = self.get_database_connection()
        try:
            for chunk in pd.read_sql(sql, con=conn, chunksize=chunksize, dtype_backend="pyarrow", **read_options):
                yield chunk
        except Exception as e:
            raise RuntimeError(f'Could not read from {name}: {e}')

    def write_to_sql(self, df: pd.DataFrame, table_name: str, write_options: dict={}) -> None:
        conn = self.get_database_connection()
        try:
            df.to_sql(table_name, con=conn, index=False, if_exists='append', **write_options)
        except Exception as e:
            raise RuntimeError(f'Could not successfully insert rows to {table_name}: {e}')

class OracleConnector(BaseRDMSConnector):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

class PostgresConnector(BaseRDMSConnector):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

class MySQLConnector(BaseRDMSConnector):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

RDBMSConnectionMapper = {
    "oracle": OracleConnector,
    "postgres": PostgresConnector,
    "mysql": MySQLConnector,
    "mariadb": MySQLConnector
}