"""
Create the daily pdf and upload to GitHub.
"""
import datetime
import os
import sys
import time

import civis
import pandas as pd
import papermill as pm 

sys.path.append(os.getcwd())

output_path = './coronavirus-stats.ipynb'

# Try executing the notebook. If it fails due to data being incomplete,
# try again in an hour, for a maximum of ten hours.
MAX_TRIES = 10
RETRY = 60 * 60
for i in range(MAX_TRIES):
    try:
        pm.execute_notebook(
           '/app/notebooks/county-city-indicators.ipynb',
           output_path,
           cwd='/app/notebooks'
        )
        break
    except pm.PapermillExecutionError as e:
        if "Data incomplete" in e.evalue:
            print(f"Data incomplete, trying again in {RETRY} seconds")
            time.sleep(RETRY)
        else:
            raise e
else:
    raise RuntimeError(f"Unable to get fresh data after {MAX_TRIES} tries.")

# shell out, run NB Convert 
output_format = 'PDFviaHTML'
cmd  = f"jupyter nbconvert --to {output_format} --no-input --no-prompt {output_path}"

output_file = './coronavirus-stats.pdf'

os.system(cmd)

