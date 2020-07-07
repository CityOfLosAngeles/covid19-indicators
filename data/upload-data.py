"""
Upload the CSV to GitHub.
We will continue to use parquets in S3.
Let's check in CSV for others.
"""
import base64
import civis
import fsspec
import os
import pandas as pd
import requests

# Constants for loading the file to master branch
TOKEN = os.environ["GITHUB_TOKEN_PASSWORD"]
BASE = "https://api.github.com"
REPO = "CityOfLosAngeles/covid19-indicators"
BRANCH = "schedule-csv"
S3_FILE_PATH = "s3://public-health-dashboard/jhu_covid19/"

def upload_file(file_name):
    PATH = f"data/{file_name}.csv"

    # Get the sha of the previous version
    r = requests.get(
        f"{BASE}/repos/{REPO}/contents/{PATH}",
        params={"ref": BRANCH},
        headers={"Authorization": f"token {TOKEN}"},
    )
    r.raise_for_status()
    sha = r.json()["sha"]

    # Upload the new version
    MY_FILE = f"{S3_FILE_PATH}{file_name}.csv"

    with fsspec.open(MY_FILE, "rb") as f:
        contents = f.read()

    r = requests.put(
        f"{BASE}/repos/{REPO}/contents/{PATH}",
        headers={"Authorization": f"token {TOKEN}"},
        json={
            "message": "Update data",
            "committer": {
                "name": "Los Angeles ITA data team",
                "email": "ITAData@lacity.org",
            },
            "branch": "schedule-csv",
            "sha": sha,
            "content": base64.b64encode(contents).decode("utf-8"),
        },
    )
    r.raise_for_status()

upload_file("city-of-la-cases")