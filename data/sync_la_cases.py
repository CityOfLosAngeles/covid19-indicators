"""
Pull data from DAILY COVID TABLE for
Mayor's Office Daily COVID-19 Report and upload to S3
"""
import pandas as pd
from datetime import date

today_date = date.today()

bucket_name = "public-health-dashboard"
S3_FILE_PATH = f"s3://{bucket_name}/jhu_covid19/"

workbook = (
    "https://docs.google.com/spreadsheets/d/"
    "1Vk7aGL7O0ZVQRySwh6X2aKlbhYlAR_ppSyMdMPqz_aI/"
    "export?format=xlsx&#gid=0"
)
sheet_name = "CASE_DATA"


def get_data(workbook, sheet_name):
    df = pd.read_excel(workbook, sheet_name=sheet_name)

    keep_cols = ["Unnamed: 0", "City of LA Cases", "City of LA New Cases", 
                "LA_CITY_DEATHS", "LA_CITY_NEW_DEATHS"]
    
    df = df[keep_cols].assign(
        Date = pd.to_datetime(df["Unnamed: 0"])
    )

    """
    These columns are read in with Excel formulas...
    There is some reallocation of new_cases and new_deaths.
    If cases are stable over 3 days, then the new_cases is reallocated and split across 3 days. 
    Valid approach used by experts, especially since confirmed cases are a function of tests conducted.
    However, we already use a 7-day rolling average to smooth out the day to day fluctuations.

    What are the implications of splitting across days, then doing a rolling average on top of that?
    Numbers will seem even more deflated?

    For now, let's create new_cases and new_deaths again, and then apply 7-day rolling average.
    Have City of LA be consistent with LA County reporting.
    """

    df = df.rename(columns = {
            "City of LA Cases": "city_cases",
            "City of LA New Cases": "city_new_cases",
            "LA_CITY_DEATHS": "city_deaths",
            "LA_CITY_NEW_DEATHS": "city_new_deaths",
            "Date": "date"
            }
        )
            
    df = (df.drop(columns = ["city_new_cases", "city_new_deaths"])
            .assign(
                date = pd.to_datetime(df.date).dt.date,
                city_cases = df.city_cases.round(0).astype("Int64"),
                city_deaths = df.city_deaths.round(0).astype("Int64"),
            )
    )

    df = df.assign(
        city_new_cases = df.sort_values("date")["city_cases"].diff(periods=1).astype("Int64"),
        city_new_deaths = df.sort_values("date")["city_deaths"].diff(periods=1).astype("Int64"),
    )

    df = (df[df.date <= today_date]
            .dropna(subset = ["city_cases", "city_deaths"])
            .sort_values("date")
            .reset_index(drop=True)
    )

    df.to_csv(f"{S3_FILE_PATH}city-of-la-cases.csv", index=False)
    df.to_parquet(f"{S3_FILE_PATH}city-of-la-cases.parquet")


def update_la_cases_data(**kwargs):   
    get_data(workbook, sheet_name)