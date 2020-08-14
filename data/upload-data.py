"""
Upload the CSV to GitHub.
We will continue to use parquets in S3.
Let's check in CSV for others.
"""
#import base64
import civis
#import fsspec
import os
#import pandas as pd
#import requests

from civis_aqueduct_utils.github import upload_file_to_github, DEFAULT_COMMITTER

# Constants for loading the file to master branch
TOKEN = os.environ["GITHUB_TOKEN_PASSWORD"]
REPO = "CityOfLosAngeles/covid19-indicators"
BRANCH = "master"
S3_FILE_PATH = "s3://public-health-dashboard/jhu_covid19/"
COMMIT_MESSAGE = "Update data"

datasets = [
    "city-of-la-cases.csv", 
    "county-city-testing.csv", 
    "hospital-availability.csv", 
    "la-county-neighborhood-time-series.parquet", 
    "la-county-neighborhood-time-series.csv", 
    "ca-hospital-and-surge-capacity.csv"
]

for file_name in datasets:
    upload_file_to_github(
        TOKEN,
        REPO,
        BRANCH,
        f"data/{file_name}",
        f"{S3_FILE_PATH}{file_name}",
        f"{COMMIT_MESSAGE}",
        DEFAULT_COMMITTER,
    )
"""
def upload_file(file_name):
    PATH = f"data/{file_name}"

    # Get the sha of the previous version
    r = requests.get(
        f"{BASE}/repos/{REPO}/contents/{PATH}",
        params={"ref": BRANCH},
        headers={"Authorization": f"token {TOKEN}"},
    )
    r.raise_for_status()
    sha = r.json()["sha"]

    # Upload the new version
    MY_FILE = f"{S3_FILE_PATH}{file_name}"

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
            "branch": BRANCH,
            "sha": sha,
            "content": base64.b64encode(contents).decode("utf-8"),
        },
    )
    r.raise_for_status()


# LA county or city data
upload_file("city-of-la-cases.csv")
upload_file("county-city-testing.csv")
upload_file("hospital-availability.csv")
upload_file("la-county-neighborhood-time-series.parquet")
# CA open data portal data
upload_file("ca-hospital-and-surge-capacity.csv")
"""