"""
Pull data from MOPS COVID Dashboard and upload to ESRI.
"""
import datetime
import os
import pytz
import arcgis
import pandas as pd
from arcgis.gis import GIS

bucket_name = "public-health-dashboard"
arcuser = os.environ.get('ARC_SERVICE_USER_NAME') 
arcpassword = os.environ.get('ARC_SERVICE_USER_PASSWORD') 

filename = "COVID_testing_data.csv"
workbook = ("https://docs.google.com/spreadsheets/d/"
"1agPpAJ5VNqpY50u9RhcPOu7P54AS0NUZhvA2Elmp2m4/"
"export?format=xlsx&#gid=0")
sheet_name = "DUPLICATE OF MOPS"


def get_county_data(filename, workbook, sheet_name):
    df = pd.read_excel(workbook, sheet_name=sheet_name, skiprows=1, index_col=0)
    df = df.T

    select_rows = [1, 2, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17]
    df = df.iloc[:, select_rows]

    df.reset_index(level=0, inplace=True)

    column_names = [
        "Date",
        "Performed",
        "Test Kit Inventory",
        "Hotkin_Memorial",
        "Hansen_Dam",
        "Crenshaw_Christian",
        "VA_Lot15",
        "Lincoln_Park",
        "West_Valley_Warner",
        "Carbon_Health_Echo_Park",
        "Kedren_Health",
        "Baldwin_Hills_Crenshaw",
        "Northridge_Fashion",
        "Nursing_Home_Outreach",
        "Homeless_Outreach",
    ]

    df.columns = column_names

    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df[
        df.Date.dt.date
        < datetime.datetime.now()
        .astimezone(pytz.timezone("America/Los_Angeles"))
        .date()
    ].sort_values("Date")

    # Fill in missing values with 0's for city sites
    city_sites = [
        "Hotkin_Memorial",
        "Hansen_Dam",
        "Crenshaw_Christian",
        "VA_Lot15",
        "Lincoln_Park",
        "West_Valley_Warner",
        "Carbon_Health_Echo_Park",
        "Kedren_Health",
        "Baldwin_Hills_Crenshaw",
        "Northridge_Fashion",
        "Nursing_Home_Outreach",
        "Homeless_Outreach",
    ]

    df[city_sites] = df[city_sites].fillna(0).astype(int)

    df = df.assign(City_Performed=df[city_sites].astype(int).sum(axis=1),)

    # Calculate cumulative sums for whole county and city
    keep_cols = ["Date", "Performed", "Cumulative", "City_Performed", "City_Cumulative"]

    df = df.assign(
        Performed=df.Performed.astype(int),
        Cumulative=df.sort_values("Date")["Performed"].cumsum().astype(int),
        City_Cumulative=df.sort_values("Date")["City_Performed"].cumsum().astype(int),
    )[keep_cols].sort_values("Date")

    return df


def update_covid_testing_city_county_data(**kwargs):
    """
    The actual python callable that Airflow schedules.
    """
    df = get_county_data(filename, workbook, sheet_name)
    df.to_csv(f"s3://{bucket_name}/jhu_covid19/county-city-cumulative.csv", index=False)
    df.to_parquet(f"s3://{bucket_name}/jhu_covid19/county-city-cumulative.parquet")