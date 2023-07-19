import sqlalchemy
from typing import Any, Dict, Union, NamedTuple
from sqlalchemy.engine.base import Connection
from sqlalchemy.engine import Engine

class ConnectionInfo(NamedTuple):
    dialect: str
    host: str
    port: Union[str, int]
    database: str
    username: str
    password: str
    engine_options: Dict[str, Any]

class BaseRDMSConnector:
    def __init__(self, conn_info: ConnectionInfo, server_side_cursors: bool=True, stream_results: bool=True) -> None:
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

class OracleConnector(BaseRDMSConnector):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)