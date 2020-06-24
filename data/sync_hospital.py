"""
Pull data from Bed Availability Google Sheet and upload to S3
"""
import datetime
import os
import pandas as pd

bucket_name = "public-health-dashboard"

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
    df.to_csv(f"s3://{bucket_name}/jhu_covid19/hospital-availability.csv")
    df.to_parquet(f"s3://{bucket_name}/jhu_covid19/hospital-availability.parquet")


def update_bed_availability_data(**kwargs):
    """
    The actual python callable that Airflow schedules.
    """
    # Getting data from google sheet
    get_data(workbook)