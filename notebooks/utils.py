"""
Functions to create county or state-specific indicators.
Use JHU county data.
Specific City of LA data also used to generate LA-specific charts. 
"""
import numpy as np
import pandas as pd
import pytz

import default_parameters
import make_charts
import useful_dict

from datetime import date, datetime, timedelta
from IPython.display import display, Markdown

s3_file_path = "s3://public-health-dashboard/jhu_covid19/"

US_COUNTY_URL = f"{s3_file_path}us-county-time-series.parquet"

LA_CITY_URL = f"{s3_file_path}city-of-la-cases.parquet"

TESTING_URL = f"{s3_file_path}county-city-cumulative.parquet"

HOSPITAL_URL = f"{s3_file_path}hospital-availability.parquet"

PPE_URL = f"{s3_file_path}ca-ppe.parquet"

HOSPITAL_SURGE_URL = f"{s3_file_path}ca-hospital-and-surge-capacity.parquet"

CROSSWALK_URL = (
    "https://raw.githubusercontent.com/CityOfLosAngeles/aqueduct/master/dags/"
    "public-health/covid19/msa_county_pop_crosswalk.csv"
)

#---------------------------------------------------------------#
# Default parameters
#---------------------------------------------------------------#
start_date = default_parameters.start_date
yesterday_date = default_parameters.yesterday_date
today_date = default_parameters.today_date
fulldate_format = default_parameters.fulldate_format
monthdate_format = default_parameters.monthdate_format


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
    # 'date' column is not JSON serializable...
    # If we drop and use date2, chart will work (https://github.com/altair-viz/altair/issues/1355) 
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
    df = pd.read_parquet(US_COUNTY_URL)
    df = df.assign(
        date=pd.to_datetime(df.date).dt.date,
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
    
    return df


def calculate_rolling_average(df):
    # Derive new columns
    df = df.assign(
        cases_avg7=df.new_cases.rolling(window=7).mean(),
        deaths_avg3=df.new_deaths.rolling(window=3).mean(),
        deaths_avg7=df.new_deaths.rolling(window=7).mean(),
    )    
    
    # Subset from start date up to yesterday's date
    # Have version of date that we can use in chart
    df = df.assign(
        date2 = pd.to_datetime(df.date)
    )   
    
    df = df[(df.date >= start_date) & (df.date < today_date)]
    
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
    city_df = pd.read_parquet(LA_CITY_URL)
    city_df["date"] = city_df["date"].astype(str).apply(lambda x: datetime.strptime(x, "%Y-%m-%d").date())    

    df = (
        city_df.rename(
            columns={"city_cases": "cases", 
                     "city_new_cases": "new_cases",
                     }
        )
        .sort_values("date")
        .reset_index(drop=True)
    )

    # Derive new columns
    df = df.assign(
        # 7-day rolling average for new cases
        cases_avg7=df.new_cases.rolling(window=7).mean(),
        date2 = pd.to_datetime(df.date),
    )
    
    # Subset by start date up to yesterday's date    
    df = df[(df.date >= start_date) & (df.date < today_date)]
    
    return df


#---------------------------------------------------------------#
# Testing Data (LA County and City of LA)
#---------------------------------------------------------------#
def lacounty_testing_charts(start_date, lower_bound, upper_bound):
    df = prep_testing(start_date)
    plot_col = "Performed"
    chart_title = "LA County: Daily Testing"
    make_charts.make_la_testing_chart(df.drop(columns="date"), plot_col, chart_title, lower_bound, upper_bound)
    return df


def lacity_testing_charts(start_date, lower_bound, upper_bound):
    df = prep_testing(start_date)
    plot_col = "City_Performed"
    chart_title = "City of LA: Daily Testing"
    make_charts.make_la_testing_chart(df.drop(columns="date"), plot_col, chart_title, lower_bound, upper_bound)
    return df


"""
Sub-functions for testing data.
"""
def prep_testing(start_date):
    df = pd.read_parquet(TESTING_URL)

    df = df.assign(
        date=df.Date.astype(str).apply(lambda x: datetime.strptime(x, "%Y-%m-%d").date()),
        date2 = pd.to_datetime(df.Date),
    )
    
    # Subset by start date up to yesterday's date    
    df = (df[(df.date >= start_date) & (df.date < today_date)]
            .drop(columns = "Date")
         )

    return df 


#---------------------------------------------------------------#
# Share of Positive Tests by Week (LA County)
#---------------------------------------------------------------#
def lacounty_positive_test_charts(start_date, positive_lower_bound, positive_upper_bound, 
                                  testing_lower_bound, testing_upper_bound):
    df = prep_la_positive_test(start_date, "county")
    chart_title1 = "LA County: Share of Positive Results by Week"
    chart_title2 = "LA County: Number of Tests & Positive Results by Week"
    make_charts.make_la_positive_test_chart(df.drop(columns = "start_of_week"), 
                                            positive_lower_bound, positive_upper_bound,
                                            testing_lower_bound, testing_upper_bound,
                                            chart_title1, chart_title2)
    return df


def lacity_positive_test_charts(start_date, positive_lower_bound, positive_upper_bound,
                               testing_lower_bound, testing_upper_bound):
    df = prep_la_positive_test(start_date, "city")
    chart_title1 = "City of LA: Share of Positive Results by Week"
    chart_title2 = "City of LA: Number of Tests & Positive Results by Week"    
    make_charts.make_la_positive_test_chart(df.drop(columns = "start_of_week"), 
                                            positive_lower_bound, positive_upper_bound,
                                            testing_lower_bound, testing_upper_bound,
                                            chart_title1, chart_title2)
    return df


"""
Sub-functions for share of positive test results data.
Combine testing data and case data, aggregated to the week.
We lack results for positive/negative results for each test batch (ideal).
"""
def prep_la_positive_test(start_date, city_or_county):
    tests_df = prep_testing(start_date)

    if city_or_county == "county":
        cases_df = prep_county("Los Angeles, CA", start_date)
        tests_col = "Performed"
    
    if city_or_county == "city":
        cases_df = prep_lacity_cases(start_date)
        tests_col = "City_Performed"
    
    #  Merge and rename columns
    df = pd.merge(cases_df, tests_df, on = "date", how = "left")
    df = df.rename(columns = {tests_col: "new_tests"}) 
    
    df = aggregate_to_week(df)
    
    return df   
    
    
def aggregate_to_week(df): 
    # Subset to particular start and end date
    df = (df[(df.date >= start_date) & (df.date < today_date)]
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
            week2 = pd.to_datetime(df.start_of_week).dt.strftime(monthdate_format),
        )
    )
    
    return df

 
#---------------------------------------------------------------#
# Hospital Equipment Availability (LA County)
#---------------------------------------------------------------#
def lacounty_hospital_charts(start_date):
    df = prep_lacounty_hospital(start_date)
    make_charts.make_lacounty_hospital_chart(df.drop(columns="date"))
    return df


"""
Sub-functions for LA County hospital equipment data.
"""
def prep_lacounty_hospital(start_date):
    df = pd.read_parquet(HOSPITAL_URL)

    # Set the denominators for each type 
    # Source: http://file.lacounty.gov/SDSInter/dhs/1070069_HavBedSummary.pdf
    # 17,000 Acute Care beds; 2,500 ICU beds; 3,199 ventilators
    def available(row):
        if "Acute" in row.equipment: 
            return 17_000
        if "ICU" in row.equipment:
            return 2_500
        if "Ventilator" in row.equipment:
            return 3_200

    # Get a total count of equipment for each date-type
    df = df.assign(
        date=df.date.apply(lambda x: datetime.strptime(x, "%m/%d/%Y").date()),
        date2 = pd.to_datetime(df.date),
        equipment_total=df.apply(available, axis=1),
    )

    # Calculate number and percent available
    sort_col = ["equipment", "date"]
    group_col = ["equipment"]
    
    df = df.assign(
        n_available=df.apply(
            lambda row: row.num if row.status == "Available" else np.nan, axis=1
        ),
        pct_available=df.apply(
            lambda row: row.num / row.equipment_total if row.status == "Available" else np.nan, axis=1
        ),
        covid_investigation=df.apply(
            lambda row: row.num if row.status == "COVID Person Under Investigation" else np.nan, axis=1
        ),
        covid_occupied=df.apply(
            lambda row: row.num if row.status == "COVID Occupied" else np.nan, axis=1
        ),
    )

    keep_col = ["date", "date2", "equipment", "equipment_total", 
                "n_available", "pct_available", 
                "covid_investigation", "covid_occupied"]
    
    for col in ["n_available", "pct_available", "covid_investigation", "covid_occupied"]:
        df[col] = df.groupby(sort_col)[col].transform("max")
    
    df = (df[keep_col]
        .drop_duplicates()
        .sort_values(sort_col)
        .reset_index(drop=True)
    )

    # Get COVID-occupied equipment investigation + occupied, or probable + confirmed
    df = df.assign(
        covid_investigation = df.covid_investigation.fillna(0).astype(int),
        covid_occupied = df.covid_occupied.fillna(0).astype(int),
    )

    df['covid'] = df.covid_investigation + df.covid_occupied
    df = df.assign(
        pct_covid = df.covid / df.equipment_total
    )

    # Calculate 3-day average 
    df = df.assign(
            n_available_avg3 = (df.groupby(group_col)['n_available']
                                .rolling(window=3).mean()
                                .reset_index(drop=True)),
            pct_available_avg3 = (df.groupby(group_col)['pct_available']
                                .rolling(window=3).mean()
                                  .reset_index(drop=True)),
            n_covid_avg7 = (df.groupby(group_col)['covid']
                        .rolling(window=7).mean()
                        .reset_index(drop=True)),
            pct_covid_avg7 = (df.groupby(group_col)['pct_covid']
                        .rolling(window=7).mean()
                        .reset_index(drop=True)),                        
        )

    df = (df[(df.n_available.notna()) & (df.date >= start_date) & (df.date < today_date)]
          .sort_values(sort_col)
          .reset_index(drop=True))

    return df


#---------------------------------------------------------------#
# COVID-Hospitalizations (CA data portal)
#---------------------------------------------------------------#
def county_covid_hospital_charts(county_state_name, start_date):
    df = prep_hospital_surge(county_state_name, start_date)
    df2 = make_long(df)
    county_name = df2.county.iloc[0]
    make_charts.make_county_covid_hospital_chart(df2.drop(columns="date"), county_name)
    return df


"""
Sub-functions for county hospitalizations data.
"""
def prep_hospital_surge(county_state_name, start_date):
    df = pd.read_parquet(HOSPITAL_SURGE_URL)
    
    df = (df.assign(
            date = pd.to_datetime(df.date).dt.date,
            date2 = pd.to_datetime(df.date),
        ).rename(columns = {"county_fips": "fips"})
    )
    
    # Parse the county_state_name into county_name and state_name (abbrev)
    if "," in county_state_name:
        county_name = county_state_name.split(",")[0].strip()

    # County names don't have " County" at the end. There is a TriCounty, UT though.
    if " County" in county_name:
        county_name = county_name.replace(" County", "").strip()

    elif any(map(str.isdigit, county_state_name)):
        county_name = df[df.fips == county_state_name].county.iloc[0]
    
    keep_cols = [
        "county",
        "fips",
        "date",
        "date2",
        "hospitalized_covid",
        "all_hospital_beds",
        "icu_covid",
        "all_icu_beds",
        "surge_available_beds"
    ]

    df = (
        df[(df.county == county_name) & 
           (df.date >= start_date)
          ][keep_cols]
        .sort_values(["county", "fips", "date"])
        .reset_index(drop=True)
    )
    
    # Calculate 3-day average
    df = df.assign(
        hospitalized_avg3 = df.hospitalized_covid.rolling(window=3).mean(),
        icu_avg3 = df.icu_covid.rolling(window=3).mean(),
        # Surge capacity has too many NaNs...can't calculate rolling average
        #surge_available_avg3 = df.surge_available_beds.rolling(window=3).mean(),
    )

    return df


# Make the df long so that we can get the encoding to show up in altair chart
def make_long(df):
    keep = ["county", "fips", "date", "date2",
           "hospitalized_avg3", "icu_avg3"]
    df2 = (df[keep]
           .rename(columns = {"hospitalized_avg3": "All COVID-Hospitalized",
                             "icu_avg3": "COVID-ICU"})
          )
    df2 = pd.melt(df2, id_vars=["county", "fips", "date", "date2"], 
                  value_vars=["All COVID-Hospitalized", "COVID-ICU"], 
                  var_name = "type", value_name = "num")
    
    df2 = df2.assign(
        num = pd.to_numeric(df2["num"], errors="coerce")
    )
    
    return df2