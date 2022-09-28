#!/usr/bin/env python
"""Covid 19 indicators dataflow implementation

Implements stage 1
"""

import binascii
import argparse
import json
import logging
import time
from typing import Any, Dict, List

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam.transforms.window as window

def string_to_hex(s):
    s2=bytearray(s,'utf8')
    return binascii.b2a_hex(s2)

def hex_to_string(s):
    return binascii.a2b_hex(s)

def json_to_hex(s):
    return (string_to_hex(s)).decode()

def hex_to_json(s):
    return hex_to_string(s)


class GenRunScript(beam.DoFn):
    def process(
            self,
            args_str_element):
        logging.info("in GenRunscdrit")
        logging.info(str(type(args_str_element)))
        logging.info("args_str_element="+str(args_str_element))
        env_dict=json.load(args_str_element)
        env=json.loads(args_str_element)
        env_list=env[env]
        f=open('temp_shell.sh','w')
        print("#!/bin/bash",file=f)
        for x in env_list:
            print("export "+x,file=f)
            print('eval "$RUN_SCRIPT"',file=f)
        f.close()
#        import os
#        output_stream=os.popen("bash temp_shell.sh")
#        logging.info(output_stream.read())


def run(
    args,beam_args: List[str] = None) -> None:
    """Build and run the pipeline."""
    options = PipelineOptions(beam_args, save_main_session=True, streaming=False)
    logging.info('args =')
    for x in args:
        logging.info(x)
  #  for x,y in options.get_all_options().items():
  #      logging.info(x+'='+str(y));
    

    with beam.Pipeline(options=options) as pipeline:
        init= (
            pipeline
            | "Read Parameters"
            >> beam.Create(args)
            | "Generate run script">>beam.ParDo(GenRunScript())
            )



if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--env",
        dest="env",
        help="Define environment variable. Can be specified multiple times."
    )
    args_in, unknown_beam_args = parser.parse_known_args()
    logging.info(args_in.env)
    print(args_in.env)
    hexstr=args_in.env
    #args=json.loads(hex_to_json(hexstr))
    args=vars(args_in)

    run(
        args,beam_args=unknown_beam_args
    )
