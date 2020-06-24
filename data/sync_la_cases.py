"""
Pull data from DAILY COVID TABLE for
Mayor's Office Daily COVID-19 Report and upload to S3
"""
import datetime
import os
import pandas as pd

bucket_name = "public-health-dashboard"

workbook = (
    "https://docs.google.com/spreadsheets/d/"
    "1Vk7aGL7O0ZVQRySwh6X2aKlbhYlAR_ppSyMdMPqz_aI/"
    "export?format=xlsx&#gid=0"
)
sheet_name = "CASE_DATA"

def get_data(workbook, sheet_name):
    df = pd.read_excel(workbook, sheet_name=sheet_name)
    df = df.loc[:, ["Date", "City of LA Cases", "City of LA New Cases"]]

    df = df.rename(columns = {
            "City of LA Cases": "city_cases",
            "City of LA New Cases": "city_new_cases",
            "Date": "date"}
        )
            
    df = (df.dropna(subset = ["city_cases"], how = "all")
            .assign(
                city_cases = df.city_cases.astype("Int64"),
                city_new_cases = df.city_new_cases.astype("Int64"),
            )
    )

    df.to_csv(f"s3://{bucket_name}/jhu_covid19/city-of-la-cases.csv", index=False)
    df.to_parquet(f"s3://{bucket_name}/jhu_covid19/city-of-la-cases.parquet")


def update_la_cases_data(**kwargs):
    """
    The actual python callable that Airflow schedules.
    """    
    get_data(workbook, sheet_name)