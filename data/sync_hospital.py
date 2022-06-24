"""
Pull data from Bed Availability Google Sheet and upload to S3
"""
import datetime
import pandas as pd

from processing_utils import default_parameters

S3_FILE_PATH = default_parameters.S3_FILE_PATH

workbook = (
    "https://docs.google.com/spreadsheets/d/"
    "1rS0Vt-kuxwQKoqZBcaOYOOTc5bL1QZqAqqPSyCaMczQ/"
    "export?format=csv&#gid=156644705"
)

def get_data(workbook):
    df = pd.read_csv(workbook)

    keep = ["date", "equipment", "status", "num"]
    df = (df.assign(
        Count = df.Count.astype("Int64")
        ).rename(columns = {
            "Date": "date",
            "Type":"equipment",
            "Status":"status",
            "Count":"num",
            }
        )[keep]
    )   
    df.to_csv(f"{S3_FILE_PATH}hospital-availability.csv", index=False)
    df.to_parquet(f"{S3_FILE_PATH}hospital-availability.parquet")


def update_bed_availability_data(**kwargs):
    get_data(workbook)