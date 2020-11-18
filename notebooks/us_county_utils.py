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

start_date = default_parameters.start_date
today_date = default_parameters.today_date

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