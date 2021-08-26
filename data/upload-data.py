"""
Upload the CSV to GitHub.
We will continue to use parquets in S3.
Let's check in CSV for others.
"""
import os

from civis_aqueduct_utils.github import upload_file_to_github
from processing_utils import default_parameters

# Constants for loading the file to master branch
TOKEN = os.environ["GITHUB_TOKEN_PASSWORD"]
REPO = "CityOfLosAngeles/covid19-indicators"
BRANCH = "master"
S3_FILE_PATH = default_parameters.S3_FILE_PATH

DEFAULT_COMMITTER = {
    "name": "Los Angeles ITA data team",
    "email": "ITAData@lacity.org",
}


datasets = [
    "city-of-la-cases.csv", 
    "county-city-testing.csv", 
    "la-county-neighborhood-time-series.csv", 
    "ca-hospital-and-surge-capacity.csv",
    "us-county-time-series.parquet",
    "global-time-series.csv",
]


for file_name in datasets:
    upload_file_to_github(
        TOKEN,
        REPO,
        BRANCH,
        f"data/{file_name}",
        f"{S3_FILE_PATH}{file_name}",
        f"Update {file_name}",
        DEFAULT_COMMITTER,
    )

print("Successful upload of datasets to GitHub")