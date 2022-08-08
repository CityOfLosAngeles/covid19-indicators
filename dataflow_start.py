#!/usr/bin/env python
"""Covid 19 indicators dataflow implementation

Implements stage 1
"""

import argparse
import json
import logging
import time
from typing import Any, Dict, List

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam.transforms.window as window

Class GenRunScript(beam.DoFn):
    def process(
            self,
            args_str_element):
        env_dict=json.load(args_str_element)
        env=json.loads(args_str_element)
        env_list=env[env]
        f=open('temp_shell.sh','w')
        print("#!/bin/bash",file=f)
        for x in env_list:
            print("export "+x,file=f)
            print('eval "$RUN_SCRIPT"',file=f)
        import os
        output_stream=os.popen("bash temp_shell.sh")
        logging.info(output_stream.read())


def run(
    args,beam_args: List[str] = None,
) -> None:
    """Build and run the pipeline."""
    options = PipelineOptions(beam_args, save_main_session=True, streaming=True)
    
    args_str=json.dumps(args)

    with beam.Pipeline(options=options) as pipeline:
        init= (
            pipeline
            | "Read Parameters"
            >> beam.Create(args_str)
            | "Generate run script">>beam.ParDo(GenRunScript()

            )



if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--env",
        dest="env",
        action="append",
        help="Define environment variable. Can be specified multiple times."
    )
    args, unknown_beam_args = parser.parse_known_args()

    run(
        args,beam_args=unknown_beam_args
    )
