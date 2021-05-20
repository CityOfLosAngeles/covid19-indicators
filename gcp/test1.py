"""
list environment variables
"""

import base64
import datetime
import fsspec
import os
import requests
import subprocess
import sys
import time

import pandas as pd
import papermill as pm 

for x in os.environ:
    print((x,os.getenv(x)))
print("Cloud Run auto build good")
