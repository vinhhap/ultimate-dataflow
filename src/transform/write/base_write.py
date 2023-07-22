import apache_beam as beam
import logging
from helper.argparser import RunParam

class BaseWrite(beam.DoFn):
    def __init__(self, source_sink: RunParam, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.source_sink = source_sink
        self._max_batch_size = source_sink.pipeline_options.get('chunksize', 50000)
        self._rows_buffer = []

    def start_bundle(self):
        self._rows_buffer = []

    def process(self, element, unused_create_fn_output=None):
        self._rows_buffer.append(element)
        if len(self._rows_buffer) >= self._max_batch_size:
            self._flush_batch()

    def finish_bundle(self):
        if self._rows_buffer:
            self._flush_batch()
            self._rows_buffer = []

    def _flush_batch(self):
        # Clear rows buffer
        self._rows_buffer = []
