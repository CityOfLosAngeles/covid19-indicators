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
import requests


sys.path.append(os.getcwd())

output_path = './ca-county-trends.ipynb'

pm.execute_notebook(
    '/app/notebooks/ca-counties.ipynb',
    output_path,
    cwd='/app/notebooks'
)

"""
# Try executing the notebook. If it fails due to data being incomplete,
# try again in an hour, for a maximum of ten hours.
MAX_TRIES = 10
RETRY = 60 * 60
for i in range(MAX_TRIES):
    try:
        pm.execute_notebook(
           '/app/notebooks/ca-counties.ipynb',
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
"""
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

# Constants for loading the file to GH Pages
TOKEN = os.environ["GITHUB_TOKEN_PASSWORD"]
BASE = "https://api.github.com"
REPO = "CityOfLosAngeles/covid19-indicators"
BRANCH = "gh-pages"
PATH = "ca-county-trends.html"

print(TOKEN)

# Get the sha of the previous version
r = requests.get(
    f"{BASE}/repos/{REPO}/contents/{PATH}",
    params={"ref": BRANCH},
    headers={"Authorization": f"token {TOKEN}"},
)
r.raise_for_status()
sha = r.json()["sha"]

# Upload the new version
with open(PATH, "rb") as f:
    content = f.read()

r = requests.put(
    f"{BASE}/repos/{REPO}/contents/{PATH}",
    headers={"Authorization": f"token {TOKEN}"},
    json={
        "message": "Update ca-county-trends.html",
        "committer": {
            "name": "Los Angeles ITA data team",
            "email": "ITAData@lacity.org",
        },
        "branch": "gh-pages",
        "sha": sha,
        "content": base64.b64encode(content).decode("utf-8"),
    },
)
r.raise_for_status()