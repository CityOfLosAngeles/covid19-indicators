"""
ETL for COVID-19 Data.
Pulls from Johns-Hopkins CSSE data.
"""
import logging
import os
from datetime import datetime

import arcgis
import pandas as pd
from arcgis.gis import GIS

# Civis would be able to inject these
arcuser = os.environ.get('ARC_SERVICE_USER_NAME') 
arcpassword = os.environ.get('ARC_SERVICE_USER_PASSWORD') 

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


# Feature ID for JHU global source data
JHU_GLOBAL_SOURCE_ID = "c0b356e20b30490c8b8b4c7bb9554e7c"

# Feature IDs for state/province level time series and current status
jhu_time_series_featureid = "20271474d3c3404d9c79bed0dbd48580"

# mobile_testing_site_featureid = 1624e8fb02e54d138e2b5206dac23998

max_record_count = 6_000_000

# The date at the time of execution. We choose midnight in the US/Pacific timezone,
# but then convert to UTC since that is what AGOL expects. When the feature layer
# is viewed in a dashboard it is converted back to local time.
# date = pd.Timestamp.now(tz="US/Pacific").normalize().tz_convert("UTC")


def parse_columns(df):
    """
    quick helper function to parse columns into values
    uses for pd.melt
    """
    columns = list(df.columns)

    id_vars, dates = [], []

    for c in columns:
        if (c.endswith("20") or c.endswith("21")):
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

    
    # join
    merge_cols = ["Province/State", "Country/Region", "Lat", "Long", "date"]
    df = pd.merge(cases_df, deaths_df, on=merge_cols, how="left")

    df = df.assign(
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

    return df.sort_values(sort_cols).reset_index(drop=True)


def load_jhu_global_current(**kwargs):
    """
    Loads the JHU global current data, transforms it so we are happy with it.
    """
    # Login to ArcGIS with Airflow
    #arcconnection = BaseHook.get_connection("arcgis")
    #arcuser = arcconnection.login
    #arcpassword = arcconnection.password
    gis = GIS("http://lahub.maps.arcgis.com", username=arcuser, password=arcpassword)

    # (1) Load current data from ESRI
    gis_item = gis.content.get(JHU_GLOBAL_SOURCE_ID)
    layer = gis_item.layers[1]
    sdf = arcgis.features.GeoAccessor.from_layer(layer)
    # ESRI dataframes seem to lose their localization.
    sdf = sdf.assign(
        date=sdf.Last_Update.dt.tz_localize("US/Pacific")
        .dt.normalize()
        .dt.tz_convert("UTC")
    )
    # Drop some ESRI faf
    sdf = sdf.drop(columns=["OBJECTID", "SHAPE"])

    """
    More cleaning steps -- remove for brevity
    """
    df = sdf.copy()

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
    gis = GIS("http://lahub.maps.arcgis.com", username=arcuser, password=arcpassword)

    historical_df = load_jhu_global_time_series()

    # Bring in the current date's JHU data
    today_df = load_jhu_global_current()
    coordinates = today_df[
        ["Province_State", "Country_Region", "Lat", "Long"]
    ].drop_duplicates()

    # Append
    df = historical_df.append(today_df, sort=False)

    """
    More cleaning steps -- remove for brevity
    """

    # Output to CSV
    time_series_filename = "/tmp/jhu_covid19_time_series.csv"
    df.to_csv(time_series_filename, index=False)


    # Overwrite the existing layers
    gis_item = gis.content.get(jhu_time_series_featureid)
    gis_layer_collection = arcgis.features.FeatureLayerCollection.fromitem(gis_item)
    gis_layer_collection.manager.overwrite(time_series_filename)
    gis_layer_collection.manager.update_definition({"maxRecordCount": max_record_count})

    # Clean up
    os.remove(time_series_filename)


def load_data(**kwargs):
    """
    Entry point for the DAG, loading state and county data to ESRI.
    """
    try:
        load_global_covid_data()
    except Exception as e:
        logging.warning("Failed to load global data with error: " + str(e))

