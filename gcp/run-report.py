"""
Simplified workflow.

Run notebook (which relies on `utils.py`).
Convert to HTML.
Upload to GitHub...`gh-pages` branch.

In terminal: cd /app/gcp
python run-report.py
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

sys.path.append(os.getcwd())

# Constants for loading the file to GH Pages branch
TOKEN = os.environ["GITHUB_TOKEN_PASSWORD"]
REPO = "CityOfLosAngeles/covid19-indicators"
BRANCH = "gh-pages"
COMMIT_MESSAGE = "Update test-report"

DEFAULT_COMMITTER = {
    "name": "Los Angeles ITA data team",
    "email": "ITAData@lacity.org",
}


notebooks_to_run = {
    "test-report.ipynb": "../gcp/test-report.ipynb",
}


def upload_file_to_github(
    token,
    repo,
    branch,
    path,
    local_file_path,
    commit_message,
    committer=DEFAULT_COMMITTER,
):
    """
    Parameters
    ----------
    token: str
        GitHub personal access token and corresponds to GITHUB_TOKEN
        in Civis credentials.
    repo: str
        Repo name, such as 'CityofLosAngeles/covid19-indicators`
    branch: str
        Branch name, such as 'master'
    path: str
        Path to the file within the repo.
    local_file_path: str
        Path to the local file to be uploaded to the repo, which can differ
        from the path within the GitHub repo.
    commit_message: str
        Commit message used when making the git commit.
    commiter: dict
        name and email associated with the committer.
        Defaults to ITA robot user, if another committer is not provided..
    """

    BASE = "https://api.github.com"

    # Get the sha of the previous version.
    # Operate on the dirname rather than the path itself so we
    # don't run into file size limitations.
    r = requests.get(
        f"{BASE}/repos/{repo}/contents/{os.path.dirname(path)}",
        params={"ref": branch},
        headers={"Authorization": f"token {token}"},
    )
    r.raise_for_status()
    item = next(i for i in r.json() if i["path"] == path)
    sha = item["sha"]

    # Upload the new version
    with fsspec.open(local_file_path, "rb") as f:
        contents = f.read()

    r = requests.put(
        f"{BASE}/repos/{repo}/contents/{path}",
        headers={"Authorization": f"token {token}"},
        json={
            "message": commit_message,
            "committer": committer,
            "branch": branch,
            "sha": sha,
            "content": base64.b64encode(contents).decode("utf-8"),
        },
    )
    r.raise_for_status()

    
for key, file_name in notebooks_to_run.items():
    try:
        
        pm.execute_notebook(
            f'/app/gcp/{key}',
            file_name,
            cwd='/app/gcp'
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
        name = file_name.replace(".ipynb", "").replace("../gcp/", "")
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
        os.remove(f"{html_file_name}")
        
    except: 
        print(f"Unsuccessful upload of {key}")
        pass