import logging
import pandas as pd
from transform.write.base_write import BaseWrite
import uuid
import copy
from source_sink import SFTPConnector
from helper.secret import get_secret

class WriteCsv(BaseWrite):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    def _flush_batch(self):
        sink = self.source_sink.sink
        sink_connection_secret = self.source_sink.sink_connection_secret
        pipeline_options = self.source_sink.pipeline_options
        write_options = pipeline_options.get("write_options", {})
        file_naming = write_options.get('file_naming', None)

        df = pd.DataFrame(self._rows_buffer).convert_dtypes(dtype_backend="pyarrow")
        file_path = f"{sink if sink.endswith('/') else sink + '/'}{file_naming + '-' if file_naming else ''}{uuid.uuid4()}.csv"
        sep = write_options.get('sep', '|')
        params = copy.deepcopy(write_options)
        params.pop('file_naming', None)
        params.pop('index', None)
        params.pop('sep', None)

        if sink.startswith('sftp://'):
            file_path = file_path.replace('sftp://', '')
            connector = SFTPConnector(get_secret(sink_connection_secret))
            connector.write_csv(file_path, df, sep=sep, **params)
            connector.close_connection()
        else:
            df.to_csv(file_path, sep = sep, index=False, **params)