import ast
from apache_beam.options.pipeline_options import PipelineOptions
import typing

class UltimateOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        # Sources and sinks list
        parser.add_argument(
            '--run_param',
            help='Run parameter',
            # type=ast.literal_eval,
            type=str,
            required=True
        )

class RunParam(typing.NamedTuple):
    name: str
    source: str
    source_type: str
    source_connection_secret: str
    sink: str
    sink_type: str
    sink_connection_secret: str
    pipeline_options: typing.Dict[str, typing.Any]