import logging
import pandas as pd
from transform.write.base_write import BaseWrite
import uuid
import copy

class WriteCsv(BaseWrite):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    def _flush_batch(self):
        sink = self.source_sink.sink
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

        df.to_csv(file_path, sep = sep, index=False, **params)