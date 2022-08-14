#!/usr/bin/env python
"""Covid 19 indicators dataflow implementation

Implements stage 1
"""

import json_to_hex
import argparse
import json
import logging
import time
from typing import Any, Dict, List

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam.transforms.window as window
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
print("hexstr="+hexstr)
args=json.loads(json_to_hex.hex_to_json(hexstr))
