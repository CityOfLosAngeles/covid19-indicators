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

from civis_aqueduct_utils.github import upload_file_to_github

sys.path.append(os.getcwd())

notebooks_to_run = [
    "ca-counties.ipynb", 
    "us-counties.ipynb", 
    "la-neighborhoods.ipynb"
]

output_path = [
    "./ca-county-trends.ipynb", 
    "./us-county-trends.ipynb", 
    "./la-neighborhoods-trends.ipynb"
]


for i, file_name in enumerate(notebooks_to_run):
    pm.execute_notebook(
        f'/app/notebooks/{file_name}',
        output_path[i],
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
        output_path[i],
    ])    


# Constants for loading the file to GH Pages branch
TOKEN = os.environ["GITHUB_TOKEN_PASSWORD"]
REPO = "CityOfLosAngeles/covid19-indicators"
BRANCH = "gh-pages"
COMMIT_MESSAGE = "Update county-trends"

DEFAULT_COMMITTER = {
    "name": "Los Angeles ITA data team",
    "email": "ITAData@lacity.org",
}

datasets = [
    "ca-county-trends.html", 
    "us-county-trends.html",
    "la-neighborhoods-trends.html",
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