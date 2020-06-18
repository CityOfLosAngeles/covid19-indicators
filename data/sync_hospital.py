"""
Pull data from Bed Availability Google Sheet and upload to ESRI
"""
import datetime
import os
import arcgis
import pandas as pd

bucket_name = "public-health-dashboard"
arcuser = os.environ.get('ARC_SERVICE_USER_NAME') 
arcpassword = os.environ.get('ARC_SERVICE_USER_PASSWORD') 


filename = "/tmp/Bed_Availability_Data.csv"
#arcfeatureid = "956e105f422a4c1ba9ce5d215b835951",
workbook = ("https://docs.google.com/spreadsheets/d/"
"1rS0Vt-kuxwQKoqZBcaOYOOTc5bL1QZqAqqPSyCaMczQ/"
"export?format=csv&#gid=156644705"
)

def get_data(filename, workbook):
    df = pd.read_csv(workbook)
    df.to_csv(f"s3://{bucket_name}/jhu_covid19/hospital-availability.csv")
    df.to_parquet(f"s3://{bucket_name}/jhu_covid19/hospital-availability.parquet")


def update_bed_availability_data(**kwargs):
    """
    The actual python callable that Airflow schedules.
    """
    # Getting data from google sheet
    get_data(filename, workbook)