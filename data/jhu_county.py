"""
Grab the JHU US county CSV from GitHub
and add JHU current feature layer to this.
"""
import geopandas as gpd
import numpy as np
import os
import pandas as pd

from datetime import datetime, timedelta


bucket_name = "public-health-dashboard"

# URL to JHU confirmed cases US county time series.
CASES_URL = (
    "https://github.com/CSSEGISandData/COVID-19/raw/{}/"
    "csse_covid_19_data/csse_covid_19_time_series/"
    "time_series_covid19_confirmed_US.csv"
)

# URL to JHU deaths US county time series.
DEATHS_URL = (
    "https://github.com/CSSEGISandData/COVID-19/raw/{}/"
    "csse_covid_19_data/csse_covid_19_time_series/"
    "time_series_covid19_deaths_US.csv"
)

LOOKUP_TABLE_URL = (
    "https://github.com/CSSEGISandData/COVID-19/raw/{}/"
    "csse_covid_19_data/"
    "UID_ISO_FIPS_LookUp_Table.csv"
)

# General function
#JHU_FEATURE_ID = "628578697fb24d8ea4c32fa0c5ae1843"
JHU_FEATURE_ID = (
    "https://services1.arcgis.com/0MSEUqKaxRlEPj5g/arcgis/rest/services/"
    "ncov_cases_US/FeatureServer/0/query?where=1%3D1&objectIds=&time=&"
    "geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&"
    "resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=OBJECTID%2C+"
    "Province_State%2C+Country_Region%2C+Last_Update%2C+Lat%2C+Long_%2C+Confirmed%2C+Recovered%2C+"
    "Deaths%2C+Active%2C+Admin2%2C+FIPS%2C+Combined_Key%2C+Incident_Rate%2C+People_Tested%2C+"
    "People_Hospitalized%2C+UID%2C+ISO3&returnGeometry=true&featureEncoding=esriDefault&"
    "multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&"
    "datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&"
    "returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&"
    "returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&"
    "outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&"
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


def fix_fips(df):
    def correct_county_fips(row):
        if (len(row.fips) == 4) and (row.fips != "None"):
            return "0" + row.fips
        elif row.fips == "None":
            return ""
        else:
            return row.fips
    
    df["fips"] = df.fips.astype(str)
    df["fips"] = df.apply(correct_county_fips, axis=1)

    for col in ["state", "county", "fips"]:
        df[col] = df[col].fillna("")
    
    return df


sort_cols = ["state", "county", "fips", "date"]


def load_jhu_us_time_series(branch="master"):
    """
    Loads the JHU US timeseries data, transforms it so we are happy with it.
    """
    cases = pd.read_csv(CASES_URL.format(branch))
    deaths = pd.read_csv(DEATHS_URL.format(branch))
    lookup_table = pd.read_csv(LOOKUP_TABLE_URL.format(branch))

    keep_lookup_cols = ["UID", "Population"]
    lookup_table = lookup_table[keep_lookup_cols]

    # melt cases
    id_vars, dates = parse_columns(cases)
    cases_df = pd.melt(
        cases, id_vars=id_vars, value_vars=dates, value_name="cases", var_name="date",
    )

    # melt deaths
    id_vars, dates = parse_columns(deaths)
    deaths_df = pd.melt(
        deaths, id_vars=id_vars, value_vars=dates, value_name="deaths", var_name="date",
    )

    # join
    merge_cols = [
        "UID",
        "iso2",
        "iso3",
        "code3",
        "FIPS",
        "Admin2",
        "Province_State",
        "Country_Region",
        "Lat",
        "Long_",
        "date",
    ]
    m1 = pd.merge(cases_df, deaths_df, on=merge_cols, how="left")

    df = pd.merge(m1.drop(columns="Population"), lookup_table, on="UID", how="left")

    keep_cols = [
        "Province_State",
        "Admin2",
        "FIPS",
        "Lat",
        "Long_",
        "date",
        "cases",
        "deaths",
        "Population",
    ]

    df = (
        df[keep_cols]
        .assign(
            date=pd.to_datetime(df.date)
            .dt.tz_localize("US/Pacific")
            .dt.normalize()
            .dt.tz_convert("UTC"),
        )
        .rename(
            columns={
                "FIPS": "fips",
                "Long_": "Lon",
                "Province_State": "state",
                "Admin2": "county",
            }
        )
    )

    # Fix fips
    df = fix_fips(df)

    return df.sort_values(sort_cols).reset_index(drop=True)


def load_jhu_us_current(**kwargs):
    """
    Loads the JHU US current data, transforms it so we are happy with it.
    """
    # Import data
    jhu = gpd.read_file(JHU_FEATURE_ID)

    # Create localized then normalized date column
    jhu["date"] = pd.Timestamp.now(tz="US/Pacific").normalize().tz_convert("UTC")
    jhu = clean_jhu_county(jhu)

    return jhu


# T1 Sub-functions
# (2) Bring in current JHU feature layer and clean
def clean_jhu_county(df):
    # Only keep certain columns and rename them to historical df
    keep_cols = [
        "Province_State",
        "Admin2",
        "Lat",
        "Long_",
        "Confirmed",
        "Deaths",
        "FIPS",
        "People_Tested",
        "date",
    ]

    df = df[keep_cols].rename(
        columns={
            "Confirmed": "cases",
            "Deaths": "deaths",
            "FIPS": "fips",
            "Long_": "Lon",
            "Province_State": "state",
            "Admin2": "county",
            "People_Tested": "people_tested",
        }
    )

    # Fix fips
    df = fix_fips(df)

    return df


# (3) Fill in missing stuff after appending
def fill_missing_stuff(df):
    # Standardize how New York City shows up
    df["county"] = df.apply(
        lambda row: "New York City" if row.fips == "36061" else row.county, axis=1
    )

    not_missing_coords = df[(df.Lat.notna()) & (df.Population.notna())][
        ["state", "county", "Lat", "Lon", "Population"]
    ].drop_duplicates()

    df = pd.merge(
        df.drop(columns=["Lat", "Lon", "Population"]),
        not_missing_coords,
        on=["state", "county"],
        how="left",
    )

    # Drop duplicates and keep last observation
    for col in ["cases", "deaths"]:
        df[col] = df.groupby(sort_cols)[col].transform("max")

    df = (
        df.drop_duplicates(subset=sort_cols, keep="last")
        .sort_values(sort_cols)
        .reset_index(drop=True)
    )

    return df


# (4) Calculate US state totals
def us_state_totals(df):
    state_grouping_cols = ["state", "date"]

    state_totals = df.groupby(state_grouping_cols).agg(
        {"cases": "sum", "deaths": "sum"}
    )
    state_totals = state_totals.rename(
        columns={"cases": "state_cases", "deaths": "state_deaths"}
    )

    df = pd.merge(df, state_totals, on=state_grouping_cols,)

    return df.sort_values(sort_cols).reset_index(drop=True)


# (5) Calculate change in caseloads from prior day
def calculate_change(df):
    county_group_cols = ["state", "county", "fips"]
    state_group_cols = ["state"]

    df = df.assign(
        new_cases=(
            df.sort_values(sort_cols)
            .groupby(county_group_cols)["cases"]
            .diff(periods=1)
        ),
        new_deaths=(
            df.sort_values(sort_cols)
            .groupby(county_group_cols)["deaths"]
            .diff(periods=1)
        ),
        new_state_cases=(
            df.sort_values(sort_cols)
            .groupby(state_group_cols)["state_cases"]
            .diff(periods=1)
        ),
        new_state_deaths=(
            df.sort_values(sort_cols)
            .groupby(state_group_cols)["state_deaths"]
            .diff(periods=1)
        ),
    )

    df = df.assign(
        new_cases=df.new_cases.fillna(df.cases),
        new_deaths=df.new_deaths.fillna(df.deaths),
        new_state_cases=df.new_state_cases.fillna(df.state_cases),
        new_state_deaths=df.new_state_deaths.fillna(df.state_deaths),
    )

    return df


# (6) Fix column types before exporting
def fix_column_dtypes(df):
    # Counties with zero cases are included in Jan/Feb/Mar.
    # Makes CSV huge. Drop these.
    df["obs"] = (
        df.sort_values(sort_cols).groupby(["state", "county", "fips"]).cumcount() + 1
    )
    df["nonzero_case"] = df.apply(
        lambda row: row.obs if row.cases > 0 else np.nan, axis=1
    )
    df["first_case"] = df.groupby(["state", "county", "fips"])[
        "nonzero_case"
    ].transform("min")

    df = df[df.obs >= df.first_case].drop(columns=["obs", "nonzero_case", "first_case"])

    # Calculate incident rate, which is cases per 100k
    incident_rate_pop = 100_000
    df = df.assign(incident_rate=(df.cases / df.Population * incident_rate_pop))
    
    # Coerce as Int64 with upgraded pandas
    integrify_me = ["cases", "deaths", 
                    "state_cases", "state_deaths", 
                    "new_cases", "new_deaths", 
                    "new_state_cases", "new_state_deaths",
                    "people_tested"]
    
    for col in integrify_me:
        df[col] = df[col].astype("Int64")

    # Sort columns
    col_order = [
        "county",
        "state",
        "fips",
        "date",
        "Lat",
        "Lon",
        "cases",
        "deaths",
        "incident_rate",
        "people_tested",
        "state_cases",
        "state_deaths",
        "new_cases",
        "new_deaths",
        "new_state_cases",
        "new_state_deaths",
    ]
    
    df = (df.reindex(columns=col_order)
        .sort_values(["state", "county", "fips", "date", "cases"])
    )

    return df


def append_county_time_series(**kwargs):
    """
    Load JHU's CSV and append today's US county data.
    """
    # (1) Load historical time-series
    historical_df = load_jhu_us_time_series()

    # (2) Bring in current JHU feature layer and clean
    today_df = load_jhu_us_current()

    # (3) Fill in missing stuff after appending
    us_county = historical_df.append(today_df, sort=False)
    us_county = fill_missing_stuff(us_county)

    # (4) Calculate US state totals
    us_county = us_state_totals(us_county)

    # (5) Calculate change in caseloads from prior day
    us_county = calculate_change(us_county)

    # (6) Fix column types before exporting
    final = fix_column_dtypes(us_county)

    # (7) Write to CSV and overwrite the old feature layer.
    final.to_csv(f"s3://{bucket_name}/jhu_covid19/us-county-time-series.csv")
    final.to_parquet(f"s3://{bucket_name}/jhu_covid19/us-county-time-series.parquet")