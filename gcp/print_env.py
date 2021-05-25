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

dateTimeObj = datetime.now()
timestampStr = dateTimeObj.strftime("%Y-%b-%d (%H:%M:%S.%f)")
print('Current Timestamp : ', timestampStr)
for x in os.environ:
    print((x,os.getenv(x)))
