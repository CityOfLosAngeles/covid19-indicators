"""
Create the CA cases trends report as HTML and upload to GitHub.
"""
import base64
import datetime
import os
import subprocess
import sys
import time

import civis
import pandas as pd
import papermill as pm 
#import requests

from civis_aqueduct_utils.github import upload_file_to_github

sys.path.append(os.getcwd())

output_path = './ca-county-trends.ipynb'

pm.execute_notebook(
    '/app/notebooks/ca-counties.ipynb',
    output_path,
    cwd='/app/notebooks'
)


# shell out, run NB Convert 
output_format = 'html'
subprocess.run([
    "jupyter",
    "nbconvert",
    "--to",
    output_format,
    "--no-input",
    "--no-prompt",
    output_path,
])    


# Constants for loading the file to GH Pages branch
TOKEN = os.environ["GITHUB_TOKEN_PASSWORD"]
REPO = "CityOfLosAngeles/covid19-indicators"
BRANCH = "gh-pages"
COMMIT_MESSAGE = "Update ca-county-trends"

DEFAULT_COMMITTER = {
    "name": "Los Angeles ITA data team",
    "email": "ITAData@lacity.org",
}

datasets = [
    "ca-county-trends.html", 
]

for file_name in datasets:
    upload_file_to_github(
        TOKEN,
        REPO,
        BRANCH,
        f"{file_name}",
        f"{file_name}",
        f"{COMMIT_MESSAGE}",
        DEFAULT_COMMITTER,
    )