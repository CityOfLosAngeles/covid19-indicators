"""
Functions to see if indicators are met for yesterday.
"""
import pandas as pd

import default_parameters
import utils

fulldate_format = default_parameters.fulldate_format
time_zone = default_parameters.time_zone
start_date = default_parameters.start_date
yesterday_date = default_parameters.yesterday_date
today_date = default_parameters.today_date
two_weeks_ago = default_parameters.two_weeks_ago
two_days_ago = default_parameters.two_days_ago


#---------------------------------------------------------------#
# Case Indicators (County, State, MSA, City of LA)
#---------------------------------------------------------------#    
def meet_case(geog, name, start_date):
    df = meet_case_death_prep(geog, name, start_date)
    extract_col = "days_fewer_cases"
    indicator = df.iloc[0][extract_col]
    
    return indicator

def meet_death(geog, name, start_date):
    df = meet_case_death_prep(geog, name, start_date)
    
    extract_col = "days_fewer_deaths"
    indicator = df.iloc[0][extract_col]
    
    return indicator 

def meet_lacity_case(start_date):
    df = utils.prep_lacity_cases(start_date)
    df = df[(df.date < today_date) & (df.date >= two_weeks_ago)]
    df = lacity_past_two_weeks(df)
    
    extract_col = "days_fewer_cases"
    indicator = df.iloc[0][extract_col]
    
    return indicator


"""
Sub-functions for cases / deaths
"""
def meet_case_death_prep(geog, name, start_date):
    if geog == "county":
        county_state_name = name
        df = utils.prep_county(county_state_name, start_date)
        name = df.county.iloc[0]
        group_cols = ["county", "state"]
        
    if geog == "state":
        state_name = name
        df = utils.prep_state(state_name, start_date)
        name = df.state.iloc[0]
        group_cols = ["state"]
        
    if geog == "msa":
        msa_name = name
        df = utils.prep_msa(msa_name, start_date)
        name = df.msa.iloc[0]
        group_cols = ["msa"]

        
    df = df[(df.date < today_date) & (df.date >= two_weeks_ago)]
    df = past_two_weeks(df, group_cols)  
    
    return df


def past_two_weeks(df, group_cols):
    """
    Count number of times in past 14 days where
    we had drop in cases / deaths from prior day.
    We will use 7-day rolling average for BOTH cases and deaths.
    Date two weeks ago is 15 days ago because
    we need 14 change-from-prior-day observations.
    """
    
    df = df.assign(
        delta_cases_avg7=(
            df.sort_values("date")
            .groupby(group_cols)["cases_avg7"]
            .diff(periods=1)
        ),
        delta_deaths_avg7=(
            df.sort_values("date")
            .groupby(group_cols)["deaths_avg7"]
            .diff(periods=1)
        )
    )

    df = df.assign(
        days_fewer_cases = df.apply(lambda row: 1 if row.delta_cases_avg7 < 0 
                                    else 0, axis=1),
        days_fewer_deaths = df.apply(lambda row: 1 if row.delta_deaths_avg7 < 0 
                                     else 0, axis=1),
    )

    two_week_totals = (df.groupby(group_cols)
                        .agg({"days_fewer_cases": "sum", 
                            "days_fewer_deaths": "sum"})
                        .reset_index()
                        )

    return two_week_totals
    

def lacity_past_two_weeks(df):
    df = df.assign(
        delta_cases_avg7=(
            df.sort_values("date")["cases_avg7"]
            .diff(periods=1)
        ),
    )

    df = df.assign(
        days_fewer_cases = df.apply(lambda row: 1 if row.delta_cases_avg7 < 0 
                                    else 0, axis=1),
        city = "LA"
    )

    two_week_totals = df.groupby("city").agg({"days_fewer_cases": "sum"}).reset_index()
                    
    return two_week_totals


#---------------------------------------------------------------#
# Daily Testing (LA County and City of LA)
#---------------------------------------------------------------#  
def meet_daily_testing(yesterday_date, city_or_county, lower_bound, upper_bound):
    df = utils.prep_testing(start_date)
    df = df.assign(
        date = pd.to_datetime(df.date).dt.strftime(fulldate_format)
    )
    
    if city_or_county == "county":
        extract_col = "Performed"
        
    if city_or_county == "city":
        extract_col = "City_Performed"
    
    indicator = df[df.date==yesterday_date].iloc[0][extract_col]
    
    return indicator

    
# Share of Positive Results
def meet_positive_share(yesterday_date, city_or_county, bound):
    """
    Returns red/green depending on if benchmark was met last week
    """
    if city_or_county == "county":
        df = utils.prep_la_positive_test(start_date, "county")
        
    if city_or_county == "city":
        df = utils.prep_la_positive_test(start_date, "city")
    
    extract_col = "pct_positive"
    indicator = df[df.week == df.week.max()].iloc[0][extract_col].round(2)
    

    return indicator

    
#---------------------------------------------------------------#
# Hospital Equipment (City of LA)
#---------------------------------------------------------------#  
def meet_acute(yesterday_date):
    df = meet_hospital(yesterday_date)
    extract_col = "pct_available"
    indicator = df[df.equipment.str.contains("Acute")].iloc[0][extract_col].round(2)
    return indicator


def meet_icu(yesterday_date):
    df = meet_hospital(yesterday_date)
    extract_col = "pct_available"
    indicator = df[df.equipment.str.contains("ICU")].iloc[0][extract_col].round(2)
    return indicator   
    
    
def meet_ventilator(yesterday_date):
    df = meet_hospital(yesterday_date)
    extract_col = "pct_available"
    indicator = df[df.equipment.str.contains("Ventilator")].iloc[0][extract_col].round(2)
    return indicator       
    
    
"""
Sub-functions for hospital data.
"""    
def meet_hospital(yesterday_date):
    # Noting that yesterday's date always seems to surpass benchmark
    # only to be revised downward again tomorrow. Use 2 days ago for now.
    yesterday_date = two_days_ago
    df = utils.prep_lacity_hospital(start_date)
    df = df.assign(
        date = pd.to_datetime(df.date).dt.strftime(fulldate_format)
    )
    df = df[df.date == yesterday_date]
    return df 