"""
Functions to create county or state-specific indicators.
Use JHU county data.
Specific City of LA data also used to generate LA-specific charts. 
"""
import numpy as np
import pandas as pd
import pytz

import make_charts
import useful_dict

from datetime import date, datetime, timedelta
from IPython.display import display, Markdown


US_COUNTY_URL = (
    "http://lahub.maps.arcgis.com/sharing/rest/content/items/"
    "782ca660304a4bdda1cc9757a2504647/data"
)

LA_CITY_URL = (
    "http://lahub.maps.arcgis.com/sharing/rest/content/items/"
    "7175fba373f541a7a19df56b6a0617f4/data"
)

TESTING_URL = (
    "http://lahub.maps.arcgis.com/sharing/rest/content/items/"
    "158dab4a07b04ecb8d47fea1746303ac/data"
)

HOSPITAL_URL = (
    "http://lahub.maps.arcgis.com/sharing/rest/content/items/"
    "3da1eb3e13a14743973c96b945bd1117/data"
)

CROSSWALK_URL = (
    "https://raw.githubusercontent.com/CityOfLosAngeles/aqueduct/master/dags/"
    "public-health/covid19/msa_county_pop_crosswalk.csv"
)

#---------------------------------------------------------------#
# Default parameters
#---------------------------------------------------------------#
county_state_name = "Los Angeles, CA"
state_name = "California"
msa_name = "Los Angeles-Long Beach-Anaheim, CA"

fulldate_format = "%-m/%-d/%y"
monthdate_format = "%-m/%-d"
time_zone = "US/Pacific"
start_date = pd.to_datetime("4/15/20").strftime(fulldate_format)

yesterday_date = (
    (datetime.today()
                .astimezone(pytz.timezone(f'{time_zone}'))
                .date()
        - timedelta(days=1)
    ).strftime("%-m/%-d/%y")
)
today_date = (
    datetime.today()
             .astimezone(pytz.timezone(f'{time_zone}'))
             .date()
             .strftime(fulldate_format)
)

two_weeks_ago = (
    (datetime.today()
                .astimezone(pytz.timezone(f'{time_zone}'))
                .date()
        - timedelta(days=15)
    ).strftime(fulldate_format)
)


#---------------------------------------------------------------#
# Case Data (County, State, MSA)
#---------------------------------------------------------------#
"""
Make cases and deaths chart for county / state / MSA.
Some data cleaning for by geographic level (listed in 1, 2a, 2b, 2c)
Call functions to make charts.
"""
# County Case Data
def county_case_charts(county_state_name, start_date):
    df = prep_county(county_state_name, start_date)
    name = df.county.iloc[0]
    make_charts.make_cases_deaths_chart(df, "county", name)
    return df

    
# State Case Data
def state_case_charts(state_name, start_date):
    df = prep_state(state_name, start_date)
    name = df.state.iloc[0]
    make_charts.make_cases_deaths_chart(df, "state", name)
    return df


# MSA Case Data
def msa_case_charts(msa_name, start_date):
    df = prep_msa(msa_name, start_date)
    name = df.msa.iloc[0]
    make_charts.make_cases_deaths_chart(df, "msa", name)
    return df


"""
Sub-functions for case, deaths data.
"""
# (1) Sub-function to prep all US time-series data
def prep_us_county_time_series():
    df = pd.read_csv(US_COUNTY_URL, dtype={"fips": "str"})
    df = df.assign(
        date=pd.to_datetime(df.date),
        state_abbrev=df.state.map(useful_dict.us_state_abbrev),
    )
    return df


# (2a) Sub-function to prep county data
def prep_county(county_state_name, start_date):
    df = prep_us_county_time_series()

    # Parse the county_state_name into county_name and state_name (abbrev)
    if "," in county_state_name:
        state_name = county_state_name.split(",")[1].strip()
        county_name = county_state_name.split(",")[0].strip()

        if len(state_name) > 2:
            state_name = useful_dict.us_state_abbrev[state_name]

        # County names don't have " County" at the end. There is a TriCounty, UT though.
        if " County" in county_name:
            county_name = county_name.replace(" County", "").strip()

    elif any(map(str.isdigit, county_state_name)):
        state_name = df[df.fips == county_state_name].state_abbrev.iloc[0]
        county_name = df[df.fips == county_state_name].county.iloc[0]

    keep_cols = [
        "county",
        "state",
        "state_abbrev",
        "fips",
        "date",
        "Lat",
        "Lon",
        "cases",
        "deaths",
        "new_cases",
        "new_deaths",
    ]

    df = (
        df[
            (df.county == county_name)
            & (df.state_abbrev == state_name)
        ][keep_cols]
        .sort_values(["county", "state", "fips", "date"])
        .reset_index(drop=True)
    )

    df = calculate_rolling_average(df)
    df = df[df.date >= start_date]

    return df


# (2b) Sub-function to prep state data
def prep_state(state_name, start_date):
    df = prep_us_county_time_series()

    keep_cols = [
        "state",
        "state_abbrev",
        "date",
        "state_cases",
        "state_deaths",
        "new_state_cases",
        "new_state_deaths",
    ]

    df = (
        df[(df.state == state_name) | 
           (df.state_abbrev == state_name)
        ][keep_cols]
        .sort_values(["state", "date"])
        .drop_duplicates()
        .rename(
            columns={
                "state_cases": "cases",
                "state_deaths": "deaths",
                "new_state_cases": "new_cases",
                "new_state_deaths": "new_deaths",
            }
        )
        .reset_index(drop=True)
    )
    
    df = calculate_rolling_average(df)
    df = df[df.date >= start_date]

    return df


# (2c) Sub-function to prep MSA data
def prep_msa(msa_name, start_date):
    group_cols = ["msa", "msa_pop", "date"]
    msa_group_cols = ["msa", "msa_pop"]

    # Merge county to MSA using crosswalk
    df = prep_us_county_time_series()

    pop = pd.read_csv(CROSSWALK_URL, dtype={"county_fips": "str", "cbsacode": "str"},)
    pop = pop[
        (pop.cbsatitle == msa_name)
        | (pop.cbsatitle.str.contains(msa_name))
        | (pop.cbsacode == msa_name)
    ][["cbsacode", "cbsatitle", "msa_pop", "county_fips"]].assign(msa=pop.cbsatitle)

    final_df = pd.merge(
        df, pop, left_on="fips", right_on="county_fips", how="inner", validate="m:1",
    )

    df = (
        final_df.groupby(group_cols)
        .agg({"cases": "sum", "deaths": "sum"})
        .reset_index()
    )

    # Create new cases and new deaths columns
    df = df.assign(
        new_cases=(
            df.sort_values(group_cols).groupby(msa_group_cols)["cases"].diff(periods=1)
        ),
        new_deaths=(
            df.sort_values(group_cols).groupby(msa_group_cols)["deaths"].diff(periods=1)
        ),
    )
    
    df = calculate_rolling_average(df)
    df = df[df.date >= start_date]
    
    return df


def calculate_rolling_average(df):
    # Derive new columns
    df = df.assign(
        cases_avg7=df.new_cases.rolling(window=7).mean(),
        deaths_avg3=df.new_deaths.rolling(window=3).mean(),
        deaths_avg7=df.new_deaths.rolling(window=7).mean(),
    )
    return df


#---------------------------------------------------------------#
# Case Data (City of LA)
#---------------------------------------------------------------#
def lacity_case_charts(start_date):
    df = prep_lacity_cases(start_date)
    make_charts.make_lacity_cases_chart(df)
    return df


"""
Sub-functions for City of LA case data.
"""
def prep_lacity_cases(start_date):
    city_df = pd.read_csv(LA_CITY_URL)
    city_df["Date"] = (pd.to_datetime(city_df.Date).dt.tz_localize("US/Pacific")
                       .dt.normalize()
                       .dt.tz_convert("UTC")
                      )

    df = (
        city_df.rename(
            columns={"City of LA Cases": "cases", 
                     "City of LA New Cases": "new_cases",
                    "Date": "date"}
        )
        .sort_values("date")
        .reset_index(drop=True)
    )

    # Derive new columns
    df = df.assign(
        # 7-day rolling average for new cases
        cases_avg7=df.new_cases.rolling(window=7).mean(),
    )
    
    df = df[df.date >= start_date]

    return df


#---------------------------------------------------------------#
# Testing Data (LA County)
#---------------------------------------------------------------#
def lacounty_testing_charts(start_date, lower_bound, upper_bound):
    df = prep_lacounty_testing(start_date)
    make_charts.make_lacounty_testing_chart(df, lower_bound, upper_bound)
    return df


"""
Sub-functions for City of LA testing data.
"""
def prep_lacounty_testing(start_date):
    df = pd.read_csv(TESTING_URL)
    df = df.assign(
        date=(pd.to_datetime(df.Date)
              .dt.tz_localize("US/Pacific")
              .dt.normalize()
              .dt.tz_convert("UTC")
             ),
    )
    df = (df[df.date >= start_date]
            .drop(columns = "Date")
         )

    return df 


#---------------------------------------------------------------#
# Share of Positive Tests by Week (LA County)
#---------------------------------------------------------------#
def lacounty_positive_test_charts(start_date):
    df = prep_lacounty_positive_test(start_date)
    make_charts.make_lacounty_positive_test_chart(df)
    return df


"""
Sub-functions for LA County share of positive test results data.
Combine testing data and case data, aggregated to the week.
We lack results for positive/negative results for each test batch (ideal).
"""
def prep_lacounty_positive_test(start_date):
    tests_df = prep_lacounty_testing(start_date)
    cases_df = prep_county("Los Angeles, CA", start_date)
    
    #  Merge and rename columns
    df = pd.merge(cases_df, tests_df, on = "date", how = "left")
    
    keep_cols = [
        "county", 
        "state", 
        "date",
        "cases",
        "new_cases",
        "Performed", 
    ]

    df = (df[keep_cols]
          .rename(columns = {"Performed": "new_tests"}) 
    )
    
    # Subset to particular start and end date
    df = (df[(df.date >= start_date) & (df.date <= yesterday_date)]
        .assign(
            week = pd.to_datetime(df.date).dt.strftime("%U"),
        ).sort_values("date")
    )
    
    # Aggregate to the week
    weekly_total = (df.groupby("week")
                .agg({"new_cases":"sum", 
                      "new_tests":"sum",
                      "date": "min",
                      "cases":"count",
                     })
                .reset_index()
                .rename(columns = {"new_cases":"weekly_cases", 
                                   "new_tests":"weekly_tests", 
                                   "date": "start_of_week", 
                                   "cases":"days_counted"})
               )

    df = pd.merge(df, weekly_total, on = "week", how = "inner")
    
    keep_col = [
        "week", 
        "start_of_week",
        "weekly_cases",
        "weekly_tests",
    ]
    
    # Calculate share of positive results for full weeks
    df = (df[df.days_counted==7][keep_col]
          .drop_duplicates()
          .assign(
            weekly_cases = df.weekly_cases.astype(int),
            weekly_tests = df.weekly_tests.astype(int),
            pct_positive = df.weekly_cases / df.weekly_tests,
            week2 = df.start_of_week.dt.strftime(monthdate_format).astype(str),
        )
    )
    
    return df

 
#---------------------------------------------------------------#
# Hospital Equipment Availability (City of LA)
#---------------------------------------------------------------#
def lacity_hospital_charts(start_date):
    df = prep_lacity_hospital(start_date)
    make_charts.make_lacity_hospital_chart(df)
    return df


"""
Sub-functions for City of LA hospital equipment data.
"""
def prep_lacity_hospital(start_date):
    df = pd.read_csv(HOSPITAL_URL)

    # Get a total count of equipment for each date-type
    df = df.assign(
        date=(pd.to_datetime(df.Date)
            .dt.tz_localize("US/Pacific")
            .dt.normalize()
            .dt.tz_convert("UTC")
             ),
        type_total=df.groupby(["Date", "Type"])["Count"].transform("sum"),
    )

    # Calculate number and percent available
    df = df.assign(
        n_available=df.apply(
            lambda row: row.Count if row.Status == "Available" else np.nan, axis=1
        ),
        pct_available=df.apply(
            lambda row: row.Count / row.type_total
            if row.Status == "Available"
            else np.nan,
            axis=1,
        ),
        equipment = df['Type'],
    )

    keep_col = ["date", "equipment", "type_total", "n_available", "pct_available"]

    df = df[(df.n_available.notna()) & (df.date >= start_date)][
        keep_col
    ].drop_duplicates()

    return df