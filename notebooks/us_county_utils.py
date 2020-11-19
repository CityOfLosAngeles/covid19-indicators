"""
Create a version of processed data
used in our notebooks.

Instead of selecting individual county and then
make chart, we'll clean all counties at once,
then subset.

Save it to `data` to use for our RMarkdown repo:
https://github.com/CityOfLosAngeles/covid19-rmarkdown
"""

import pandas as pd

import default_parameters
import utils

from IPython.display import Markdown, HTML

start_date = default_parameters.start_date
today_date = default_parameters.today_date
yesterday_date = default_parameters.yesterday_date
one_week_ago = default_parameters.one_week_ago

fulldate_format = default_parameters.fulldate_format


# Clean the JHU county data at once
def clean_jhu(start_date):
    df = utils.prep_us_county_time_series()

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

    df = (df[keep_cols]
        .sort_values(["county", "state", "fips", "date"])
        .reset_index(drop=True)
    )
    
    # Merge in population
    pop = (pd.read_csv(utils.CROSSWALK_URL, 
                       dtype={"county_fips": "str", "cbsacode": "str"})
           [["county_fips", "county_pop"]]
           .rename(columns = {"county_fips": "fips"})
          )

    df = pd.merge(df, pop,
                  on = "fips", how = "inner", validate = "m:1"
    )
    
    df = utils.calculate_rolling_average(df, start_date, today_date)
    df = utils.find_tier_cutoffs(df, "county_pop")
    
    return df


# Clean all CA counties hospitalizations data at once
def clean_hospitalizations(start_date):
    
    df = pd.read_parquet(utils.HOSPITAL_SURGE_URL)
    
    df = (df.assign(
            date = pd.to_datetime(df.date).dt.date,
            date2 = pd.to_datetime(df.date),
        ).rename(columns = {"county_fips": "fips"})
    )
    
    keep_cols = [
        "county",
        "fips",
        "date",
        "date2",
        "hospitalized_covid",
        "all_hospital_beds",
        "icu_covid",
        "all_icu_beds",
    ]

    df = (
        df[keep_cols]
        .sort_values(["county", "fips", "date"])
        .reset_index(drop=True)
    )
    
    # Calculate 7-day average
    df = df.assign(
        hospitalized_avg7 = df.hospitalized_covid.fillna(0).rolling(window=7).mean(),
        icu_avg7 = df.icu_covid.fillna(0).rolling(window=7).mean(),
    )

    df = df[(df.date >= start_date) & (df.date < today_date)]

    df = utils.make_long(df)
    
    return df


def county_caption(df, county_name):
    df = df[df.county == county_name]
    
    '''
    This changes the columns to string...which shows up incorrectly in Markdown.
    
    cols_to_format = ["cases", "deaths", "new_cases", "new_deaths"]
    for c in cols_to_format:
        df[c] = df[c].map("{:,g}".format)
    '''

    extract_col = "cases"    
    cumulative_cases = df[df.date == yesterday_date].iloc[0][extract_col]
    
    extract_col = "cases_avg7"
    new_cases_1week = df[df.date == one_week_ago].iloc[0][extract_col]
    new_cases_yesterday = df[df.date == yesterday_date].iloc[0][extract_col]   
    pct_change_new_cases = (((new_cases_yesterday - new_cases_1week) / new_cases_1week) * 100).round(1)
    
    
    extract_col = "deaths"
    cumulative_deaths = df[df.date == yesterday_date][extract_col].iloc[0]

    
    extract_col = "deaths_avg7"
    new_deaths_1week = df[df.date == one_week_ago].iloc[0][extract_col]
    new_deaths_yesterday = df[df.date == yesterday_date].iloc[0][extract_col]      
    pct_change_new_deaths = (((new_deaths_yesterday - new_deaths_1week) / new_deaths_1week) * 100).round(1)

    
    display(
        Markdown(
            f"As of {yesterday_date.strftime(fulldate_format)}, there were **{cumulative_cases:,}** total cases "
            f"and **{cumulative_deaths:,}** total deaths. "
            f"<br>In the past week, new cases (7-day rolling avg) grew by **{pct_change_new_cases}%**; new deaths (7-day rolling avg) grew by **{pct_change_new_deaths}%**. " 
        )
    )
    
    