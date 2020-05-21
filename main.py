"""
Entrypoint script to schedule notebook 
and send email. 

Author: @hunterowens
"""

import papermill as pm 
import pandas as pd
import datetime
import civis
import os

# check dir
if not os.path.exists('outputs'):
    os.makedirs('outputs') 

output_path = f'./outputs/{str(datetime.datetime.now().date())}-corona.ipynb'
# execute notebook 
pm.execute_notebook(
   './notebooks/coronavirus-indicators.ipynb',
   output_path,
   parameters=dict(start_date="4/15/2020")
)

# shell out, run NB Convert 

cmd  = f"jupyter nbconvert --to html --no-input --no-prompt {output_path}"

output_file = f'./outputs/{str(datetime.datetime.now().date())}-corona.html'

os.system(cmd)
# send an email with the HTML 
html = open(output_file, 'r').read().splitlines()

html = "".join(html)
#Create function that defines the "source script" of the new script that get generated (sends to emails)
def create_source_script():
    source_str = f"""import os \n
import civis \n
from datetime import date \n
client = civis.APIClient()
client.scripts.patch_python3(os.environ['CIVIS_JOB_ID'], notifications = {{
        'success_email_subject' : 'Coronavirus Indicators, {datetime.date.today().strftime("%m/%d/%Y")}',
        'success_email_body' : '{html}',
        'success_email_addresses' : ['itadata@lacity.org']}})
        """
    return source_str


#Define function that creates new script
def create_new_email_script(client):
    new_script = client.scripts.post_python3(name = 'tmp coronavirus',
                                             source = create_source_script())
    return new_script

client = civis.APIClient()

temp_job_id = create_new_email_script(client)
print(temp_job_id)

