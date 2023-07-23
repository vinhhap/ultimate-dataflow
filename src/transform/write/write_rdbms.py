import logging
from source_sink import RDBMSConnectionInfo, RDBMSConnectionMapper
from helper.secret import get_secret
import pandas as pd
from transform.write.base_write import BaseWrite

class WriteRDBMS(BaseWrite):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _flush_batch(self):
        # Extract info from class attribute
        sink = self.source_sink.sink
        sink_type = self.source_sink.sink_type
        sink_connection_secret = self.source_sink.sink_connection_secret

        # Create connector
        conn_info = RDBMSConnectionInfo(**get_secret(sink_connection_secret))
        connector = RDBMSConnectionMapper[sink_type](conn_info)

        # Convert list of dict to pd.DataFrame & write to sink
        df = pd.DataFrame(self._rows_buffer).convert_dtypes(dtype_backend="pyarrow")
        connector.write_to_sql(df=df, table_name=sink)

        # Clear rows buffer
        self._rows_buffer = []