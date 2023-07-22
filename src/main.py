import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from helper.argparser import UltimateOptions, RunParam
from transform.read_source import SourceToPColl
from transform.write_sink import PCollToSink
import logging

def run():
    p_options = {
        "experiments": ["use_runner_v2"],
        "save_main_session": True,
        "streaming": False,
        "sdk_location": "container"
    }
    pipeline_options = PipelineOptions(**p_options)
    user_options = pipeline_options.view_as(UltimateOptions)
    run_param = json.loads(user_options.run_param)
    p = beam.Pipeline(options=pipeline_options)

    for item in run_param:
        source_sink = RunParam(**item)
        initialize = p | f"Initialize {source_sink.name}" >> beam.Create([source_sink])
        read_source = initialize | f"Read {source_sink.name}" >> SourceToPColl(source_sink)
        write_sink = read_source | f"Write {source_sink.name}" >> PCollToSink(source_sink)
    logging.info("Building pipeline ...")

    p.run().wait_until_finish()
if __name__ == "__main__":
    logging.getLogger().setLevel(logging.DEBUG)
    run()