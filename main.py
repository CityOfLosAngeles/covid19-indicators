import papermill as pm 

import pandas as pd

import datetime

import os

# check dir
if not os.path.exists('outputs'):
    os.makedirs('outputs') 

output_path = f'./outputs/{str(datetime.datetime.now().date())}-corona.ipynb'
# execute notebook 
pm.execute_notebook(
   './notebooks/coronavirus-indicators.ipynb',
   output_path,
   parameters=dict(alpha=0.6, ratio=0.1)
)

# shell out, run NB Convert 

cmd  = f"jupyter nbconvert --to html --no-input --no-prompt {output_path}"

os.system(cmd)
# send an email with the HTML 