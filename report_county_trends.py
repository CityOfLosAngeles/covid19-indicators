"""
Create the CA cases trends report as HTML and upload to GitHub.
"""
import base64
import datetime
import os
import subprocess
import sys
import time

import pandas as pd
import papermill as pm 

from civis_aqueduct_utils.github import upload_file_to_github

sys.path.append(os.getcwd())

# Constants for loading the file to GH Pages branch
TOKEN = os.environ["GITHUB_TOKEN_PASSWORD"]
REPO = "CityOfLosAngeles/covid19-indicators"
BRANCH = "gh-pages"

env_list=dict(os.environ)
if "CURRENT_BRANCH" in env_list:
    CURRENT_BRANCH = os.environ["CURRENT_BRANCH"]
    search_str=CURRENT_BRANCH + "_env_PUBLISH_PATH"
    if search_str in env_list:
        PUBLISH_PATH=os.environ[search_str]
    else:
        PUBLISH_PATH="test_branch/"

DEFAULT_COMMITTER = {
    "name": "Los Angeles ITA data team",
    "email": "ITAData@lacity.org",
}


notebooks_to_run = {
    "ca-counties.ipynb": "./test-ca-county-trends.ipynb",
    "us-counties.ipynb": "./test-us-county-trends.ipynb", 
    "la-neighborhoods.ipynb": "./test-la-neighborhoods-trends.ipynb",
    "coronavirus-stats.ipynb": './test-coronavirus-stats.ipynb',
}

for key, file_name in notebooks_to_run.items():
    try:
        pm.execute_notebook(
            f'/app/notebooks/{key}',
            file_name,
            cwd='/app/notebooks',
            log_output=True
        )

        print("Ran notebook")

        # shell out, run NB Convert 
        output_format = 'html'
        subprocess.run([
            "jupyter",
            "nbconvert",
            "--to",
            output_format,
            "--no-input",
            "--no-prompt",
            file_name,
        ]) 

        print("Converted to HTML")
        # Now find the HTML file and upload
        name = file_name.replace(".ipynb", "").replace("./", "")
        html_file_name = f"{name}.html" 
        print(f"name: {name}")
        print(f"html name: {html_file_name}")

        upload_file_to_github(
            TOKEN,
            REPO,
            BRANCH,
            f"{html_file_name}",
            f"{PUBLISH_PATH}{html_file_name}",
            f"Update {name}",
            DEFAULT_COMMITTER,
        )

        print("Successful upload to GitHub")
    except: 
        print(f"Unsuccessful upload of {key}")
        pass
