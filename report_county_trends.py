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
COMMIT_MESSAGE = "Update county-trends"

DEFAULT_COMMITTER = {
    "name": "Los Angeles ITA data team",
    "email": "ITAData@lacity.org",
}


notebooks_to_run = {
    "ca-counties.ipynb": "./ca-county-trends.ipynb",
    "us-counties.ipynb": "./us-county-trends.ipynb", 
    "la-neighborhoods.ipynb": "./la-neighborhoods-trends.ipynb",
}

for key, file_name in notebooks_to_run.items():
    try:
        pm.execute_notebook(
            f'/app/notebooks/{key}',
            file_name,
            cwd='/app/notebooks'
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
            f"{html_file_name}",
            f"{COMMIT_MESSAGE}",
            DEFAULT_COMMITTER,
        )

        print("Successful upload to GitHub")
    except: 
        print(f"Unsuccessful upload of {key}")
        pass