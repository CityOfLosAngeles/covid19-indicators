"""
Pull data from DAILY COVID TABLE for
Mayor's Office Daily COVID-19 Report and upload to ESRI
"""
import datetime
import os
import arcgis
import pandas as pd

bucket_name = "public-health-dashboard"
arcuser = os.environ.get('ARC_SERVICE_USER_NAME') 
arcpassword = os.environ.get('ARC_SERVICE_USER_PASSWORD') 

workbook = ("https://docs.google.com/spreadsheets/d/"
"1Vk7aGL7O0ZVQRySwh6X2aKlbhYlAR_ppSyMdMPqz_aI/"
"export?format=xlsx&#gid=0"
)
sheet_name = "CASE_DATA"

def get_data(workbook, sheet_name):
    df = pd.read_excel(workbook, sheet_name=sheet_name)
    df = df.loc[:, ["Date", "City of LA Cases", "City of LA New Cases"]]
    df.dropna(
        subset=["City of LA Cases", "City of LA New Cases"], how="all", inplace=True
    )
    df["City of LA Cases"] = df["City of LA Cases"].astype(int)
    df["City of LA New Cases"] = df["City of LA New Cases"].astype(int)
    df.to_csv(f"s3://{bucket_name}/jhu_covid19/city-of-la-cases.csv", index=False)
    df.to_parquet(f"s3://{bucket_name}/jhu_covid19/city-of-la-cases.parquet")


def update_la_cases_data(**kwargs):
    """
    The actual python callable that Airflow schedules.
    """    
    get_data(workbook, sheet_name)