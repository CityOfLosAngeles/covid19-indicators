"""
ETL for COVID-19 Data.
Pulls from Johns-Hopkins CSSE data.
"""
import geopandas as gpd
import numpy as np
import os
import pandas as pd

from datetime import datetime, timedelta

bucket_name = "public-health-dashboard"
S3_FILE_PATH = f"s3://{bucket_name}/jhu_covid19/"

# URL to JHU confirmed cases time series.
CASES_URL = (
    "https://github.com/CSSEGISandData/COVID-19/raw/{}/"
    "csse_covid_19_data/csse_covid_19_time_series/"
    "time_series_covid19_confirmed_global.csv"
)

# URL to JHU deaths time series.
DEATHS_URL = (
    "https://github.com/CSSEGISandData/COVID-19/raw/{}/"
    "csse_covid_19_data/csse_covid_19_time_series/"
    "time_series_covid19_deaths_global.csv"
)

# URL to JHU recoveries time series
RECOVERED_URL = (
    "https://github.com/CSSEGISandData/COVID-19/raw/{}/"
    "csse_covid_19_data/csse_covid_19_time_series/"
    "time_series_covid19_recovered_global.csv"
)

# Feature ID for JHU global source data
#JHU_GLOBAL_SOURCE_ID = "c0b356e20b30490c8b8b4c7bb9554e7c"

# Can use geojson query instead
JHU_GLOBAL_SOURCE_ID = (
    "https://services1.arcgis.com/0MSEUqKaxRlEPj5g/ArcGIS/rest/services/"
    "ncov_cases/FeatureServer/0/query?where=1%3D1&objectIds=&time=&geometry=&"
    "geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&"
    "resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&"
    "outFields=OBJECTID%2C+Province_State%2C+Country_Region%2C+Last_Update%2C+Lat%2C+Long_%2C"
    "+Confirmed%2C+Recovered%2C+Deaths%2C+Active%2C+Admin2%2C+FIPS%2C+Combined_Key&"
    "returnGeometry=true&featureEncoding=esriDefault&multipatchOption=xyFootprint&"
    "maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&"
    "returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&"
    "returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&"
    "orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&"
    "resultOffset=&resultRecordCount=&returnZ=false&returnM=false&"
    "returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pgeojson&token="
)


def parse_columns(df):
    """
    quick helper function to parse columns into values
    uses for pd.melt
    """
    columns = list(df.columns)

    id_vars, dates = [], []

    for c in columns:
        if c.endswith("20"):
            dates.append(c)
        else:
            id_vars.append(c)
    return id_vars, dates


sort_cols = ["Country_Region", "Province_State", "date"]


def load_jhu_global_time_series(branch="master"):
    """
    Loads the JHU global timeseries data, transforms it so we are happy with it.
    """
    cases = pd.read_csv(CASES_URL.format(branch))
    deaths = pd.read_csv(DEATHS_URL.format(branch))
    recovered = pd.read_csv(RECOVERED_URL.format(branch))
    # melt cases
    id_vars, dates = parse_columns(cases)
    cases_df = pd.melt(
        cases,
        id_vars=id_vars,
        value_vars=dates,
        value_name="number_of_cases",
        var_name="date",
    )

    # melt deaths
    id_vars, dates = parse_columns(deaths)
    deaths_df = pd.melt(
        deaths,
        id_vars=id_vars,
        value_vars=dates,
        value_name="number_of_deaths",
        var_name="date",
    )

    # melt recovered
    id_vars, dates = parse_columns(recovered)
    recovered_df = pd.melt(
        recovered,
        id_vars=id_vars,
        value_vars=dates,
        value_name="number_of_recovered",
        var_name="date",
    )

    # join
    merge_cols = ["Province/State", "Country/Region", "Lat", "Long", "date"]
    m1 = pd.merge(cases_df, deaths_df, on=merge_cols, how="left")
    df = pd.merge(m1, recovered_df, on=merge_cols, how="left")

    df = (df.assign(
            date=pd.to_datetime(df.date)
            .dt.tz_localize("US/Pacific")
            .dt.normalize()
            .dt.tz_convert("UTC"),
        ).rename(
            columns={
                "Country/Region": "Country_Region",
                "Province/State": "Province_State",
            }
        )
    )

    return df.sort_values(sort_cols).reset_index(drop=True)


def load_jhu_global_current(**kwargs):
    """
    Loads the JHU global current data, transforms it so we are happy with it.
    """
    # Load current data from ESRI
    sdf = gpd.read_file(JHU_GLOBAL_SOURCE_ID)

    sdf = sdf.assign(
        date = (pd.to_datetime(sdf.Last_Update, unit="ms")
                .dt.tz_localize("US/Pacific")
                .dt.normalize()
                .dt.tz_convert("UTC")
            ),
    )

    # CSVs report province-level totals for every country except US.
    global_df = sdf[sdf.Country_Region != "US"]
    # US should just have 1 observation.
    us_df = sdf[sdf.Country_Region == "US"]
    us_totals = (
        us_df.groupby(["Country_Region", "date"])
        .agg({"Confirmed": "sum", "Recovered": "sum", "Deaths": "sum"})
        .assign(Lat=37.0902, Long_=-95.7129,)
        .reset_index()
    )

    us_df = pd.merge(
        us_df.drop(columns=["Lat", "Long_", "Confirmed", "Recovered", "Deaths"]),
        us_totals,
        on=["Country_Region", "date"],
        how="left",
    )

    df = global_df.append(us_totals, sort=False)

    df = df.assign(
        number_of_cases=pd.to_numeric(df.Confirmed),
        number_of_recovered=pd.to_numeric(df.Recovered),
        number_of_deaths=pd.to_numeric(df.Deaths),
    ).rename(columns={"Long_": "Long"})

    keep_cols = [
        "Province_State",
        "Country_Region",
        "date",
        "Lat",
        "Long",
        "number_of_cases",
        "number_of_recovered",
        "number_of_deaths",
    ]

    df = df[keep_cols]

    return df.sort_values(sort_cols).reset_index(drop=True)


def load_global_covid_data():
    """
    Load global COVID-19 data from JHU.
    """
    historical_df = load_jhu_global_time_series()

    # Bring in the current date's JHU data
    today_df = load_jhu_global_current()
    coordinates = today_df[
        ["Province_State", "Country_Region", "Lat", "Long"]
    ].drop_duplicates()

    # Append
    df = historical_df.append(today_df, sort=False)

    # Merge in lat/lon coordinates
    # There are differences between GitHub CSV and feature layer. Use feature layer's.
    df = pd.merge(
        df.drop(columns=["Lat", "Long"]),
        coordinates,
        on=["Province_State", "Country_Region"],
        how="left",
        validate="m:1",
    )
    
    df = (
        df.assign(
            number_of_cases=df.number_of_cases.astype("Int64"),
            number_of_deaths=df.number_of_deaths.astype("Int64"),
            number_of_recovered=df.number_of_recovered.astype("Int64"),
            Province_State = df.Province_State.fillna(""),
        )
        .drop_duplicates(subset=sort_cols, keep="last")
        .sort_values(sort_cols)
        .reset_index(drop=True)
    )

    # Output to CSV
    df.to_csv(f"{S3_FILE_PATH}global-time-series.csv", index=False)
    df.to_parquet(f"{S3_FILE_PATH}global-time-series.parquet")
