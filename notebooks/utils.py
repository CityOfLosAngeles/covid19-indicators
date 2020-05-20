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
time_zone = "US/Pacific"
start_date = "4/1/20"
yesterday_date = (
    datetime.today()
                .astimezone(pytz.timezone(f'{time_zone}'))
                .date()
        - timedelta(days=1)   
)
today_date = (
    datetime.today()
             .astimezone(pytz.timezone(f'{time_zone}'))
             .date()
)

#---------------------------------------------------------------#
# Chart parameters
#---------------------------------------------------------------#
navy = "#0A4C6A"
maroon = "#A30F23"
green = "#10DE7A"
orange = "#FCA800"

title_font_size = 10
font_name = "Roboto"
grid_opacity = 0.4
domain_opacity = 0.4
stroke_opacity = 0
time_unit = "monthdate"
chart_width = 250
chart_height = 200
bin_spacing = 20
fulldate_format = "%-m/%-d/%y"
monthdate_format = "%-m/%-d"


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
    df = make_charts.make_cases_deaths_chart(df, "county", name)
    return df

def county_case_indicators(county_state_name, start_date):
    df = prep_county(county_state_name, start_date)
    return df

    
# State Case Data
def state_case_charts(state_name, start_date):
    df = prep_state(state_name, start_date)
    name = df.state.iloc[0]
    df = make_charts.make_cases_deaths_chart(df, "state", name)
    return df

def state_case_indicators(state_name, start_date):
    df = prep_state(state_name, start_date)
    return df


# MSA Case Data
def msa_case_charts(msa_name, start_date):
    df = prep_msa(msa_name, start_date)
    name = df.msa.iloc[0]
    df = make_charts.make_cases_deaths_chart(df, "msa", name)
    return df

def msa_case_indicators(msa_name, start_date):
    df = prep_msa(msa_name, start_date)
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
            & (df.date >= start_date)
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
        df[
            ((df.state == state_name) | (df.state_abbrev == state_name))
            & (df.date >= start_date)
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
        final_df[final_df.date >= start_date]
        .groupby(group_cols)
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
    return df


#---------------------------------------------------------------#
# Case Data (City of LA)
#---------------------------------------------------------------#
def case_indicators_lacity(start_date):
    city_df = pd.read_csv(LA_CITY_URL)
    city_df["date"] = pd.to_datetime(city_df.Date)

    df = (
        city_df[city_df.date >= start_date]
        .rename(
            columns={"City of LA Cases": "cases", "City of LA New Cases": "new_cases"}
        )
        .sort_values("date")
        .reset_index(drop=True)
    )

    # Derive new columns
    df = df.assign(
        # 7-day rolling average for new cases
        cases_avg7=df.new_cases.rolling(window=7).mean(),
    )

    # Make cases charts
    cases_chart = (
        alt.Chart(df)
        .mark_line()
        .encode(
            x=alt.X("date", timeUnit=time_unit, 
                    title="date", axis=alt.Axis(format=monthdate_format)
                   ),
            y=alt.Y("cases_avg7", title="7-day avg"),
            color=alt.value(navy),
        )
        .properties(
            title="City of LA: New Cases", width=chart_width, height=chart_height
        )
        .configure_title(
            fontSize=title_font_size, font=font_name, anchor="middle", color="black"
        )
        .configure_axis(gridOpacity=grid_opacity, domainOpacity=domain_opacity)
        .configure_view(strokeOpacity=stroke_opacity)
    )

    display(cases_chart)

    return df


#---------------------------------------------------------------#
# Testing Data (City of LA)
#---------------------------------------------------------------#
def testing_lacity(start_date, daily_or_monthly, lower_bound, upper_bound):
    df = pd.read_csv(TESTING_URL)
    df = df.assign(
        Date=pd.to_datetime(df.Date).dt.strftime(fulldate_format),
        month=pd.to_datetime(df.Date).dt.month,
    )
    df = df[df.Date >= start_date]

    # Aggregate tests by month
    df = df.assign(Performed_Monthly=df.groupby("month")["Performed"].transform("sum"))

    if daily_or_monthly == "monthly":
        format_date = "%b"
        plot_col = "Performed_Monthly:Q"
        chart_title = "Monthly Tests Performed"
        df = df.drop_duplicates(subset=["month", "Performed_Monthly"])
        chart_width = 150

    if daily_or_monthly == "daily":
        format_date = monthdate_format
        plot_col = "Performed:Q"
        chart_title = "Daily Tests Performed"
        chart_width = 500

    make_testing_chart(
        df, plot_col, format_date, lower_bound, upper_bound, chart_title, chart_width
    )

    return df


# Sub-function to make testing bar chart
def make_testing_chart(
    df, plot_col, format_date, lower_bound, upper_bound, chart_title, chart_width
):
    bar = (
        alt.Chart(df)
        .mark_bar(color=navy)
        .encode(
            x=alt.X(
                "Date",
                timeUnit=time_unit,
                title="date",
                axis=alt.Axis(format=format_date),
            ),
            y=alt.Y(plot_col, title="Tests Performed"),
        )
    )

    line1 = (
        alt.Chart(pd.DataFrame({"y": [lower_bound]}))
        .mark_rule(color=maroon, strokeDash=[5, 2])
        .encode(y="y")
    )
    line2 = (
        alt.Chart(pd.DataFrame({"y": [upper_bound]}))
        .mark_rule(color=maroon, strokeDash=[5, 2])
        .encode(y="y")
    )

    testing_chart = (
        (bar + line1 + line2)
        .properties(title=chart_title, width=chart_width)
        .configure_title(
            fontSize=title_font_size, font=font_name, anchor="middle", color="black"
        )
        .configure_axis(
            gridOpacity=grid_opacity, domainOpacity=domain_opacity, ticks=False
        )
        .configure_view(strokeOpacity=stroke_opacity)
    )

    display(testing_chart)

    
#---------------------------------------------------------------#
# Share of Positive Tests by Week (City of LA)
#---------------------------------------------------------------#
def positive_tests_lacity(start_date):
    tests_df = pd.read_csv(TESTING_URL)
    cases_df = pd.read_csv(LA_CITY_URL)
    
    #  Merge and rename columns
    df = pd.merge(cases_df, tests_df, on = "Date", how = "left")
    
    keep_cols = [
        "Date",
        "City of LA Cases",
        "City of LA New Cases",
        "Performed", 
        "Cumulative",
    ]

    df = (df[keep_cols].assign(
            Date = pd.to_datetime(df.Date),
        )
          .rename(columns = 
                  {"City of LA Cases": "cases",
                   "City of LA New Cases": "new_cases",
                   "Performed": "new_tests",
                   "Cumulative": "tests",
                   "Date": "date"}) 
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

    make_positive_test_chart(df)
    
    return df
    
    
# Sub-function to make share of positive tests chart
def make_positive_test_chart(df):
    chart_title1 = "Weekly Share of Positive Results"
    chart_title2 = "Weekly Tests Conducted"
    
    positive_bar = (
        alt.Chart(df)
        .mark_bar(color = navy, binSpacing = bin_spacing)
        .encode(
            x=alt.X(
                "week2",
                title="date",
            ),
            y=alt.Y(
                "pct_positive", 
                title="Share of Positive COVID-19 Results",
                axis=alt.Axis(format="%")
            ),
        )
        .properties(title=chart_title1, width = chart_width)
        .configure_title(
            fontSize=title_font_size, font=font_name, anchor="middle", color="black"
        )
        .configure_axis(
            gridOpacity=grid_opacity, domainOpacity=domain_opacity, ticks=False
        )
        .configure_view(strokeOpacity=stroke_opacity)
    )

    test_bar = (
        alt.Chart(df)
        .mark_bar(color = orange, binSpacing = bin_spacing)
        .encode(
            x=alt.X(
                "week2",
                title="start of week date",
            ),
            y=alt.Y(
                "weekly_tests", 
                title="# Weekly Tests",
            ),
        )
        .properties(title=chart_title2, width = chart_width)
        .configure_title(
            fontSize=title_font_size, font=font_name, anchor="middle", color="black"
        )
        .configure_axis(
            gridOpacity=grid_opacity, domainOpacity=domain_opacity, ticks=False
        )
        .configure_view(strokeOpacity=stroke_opacity)
    )

    display(positive_bar)
    display(test_bar)

    
#---------------------------------------------------------------#
# Hospital Equipment Availability (City of LA)
#---------------------------------------------------------------#
def hospital_capacity_lacity(start_date):
    df = pd.read_csv(HOSPITAL_URL)

    # Get a total count of equipment for each date-type
    df = df.assign(
        Date=pd.to_datetime(df.Date).dt.strftime(fulldate_format),
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
    )

    keep_col = ["Date", "Type", "type_total", "n_available", "pct_available"]

    df = df[(df.n_available.notna()) & (df.Date >= start_date)][
        keep_col
    ].drop_duplicates()

    make_hospital_chart(df)

    return df

# Sub-function to make hospital equipment availability line chart
def make_hospital_chart(df):
    chart_width = 500
    acute_color = green
    icu_color = navy
    ventilator_color = orange

    base = (
        alt.Chart(df)
        .mark_line()
        .encode(
            x=alt.X(
                "Date",
                timeUnit=time_unit,
                title="date",
                axis=alt.Axis(format=monthdate_format),
            ),
            y=alt.Y("pct_available", title="% available", 
                    axis=alt.Axis(format="%")
            ),
            color=alt.Color(
                "Type",
                scale=alt.Scale(
                    domain=["Acute Care Beds", "ICU Beds", "Ventilators"],
                    range=[acute_color, icu_color, ventilator_color],
                ),
            ),
        )
    )

    line1 = (
        alt.Chart(pd.DataFrame({"y": [0.3]}))
        .mark_rule(color=maroon, strokeDash=[5, 2])
        .encode(y="y")
    )

    hospital_pct_chart = (
        (base + line1)
        .properties(
            title="Percent of Available Hospital Equipment by Type",
            width=chart_width,
        )
        .configure_title(
            fontSize=title_font_size, font=font_name, anchor="middle", color="black"
        )
        .configure_axis(
            gridOpacity=grid_opacity, domainOpacity=domain_opacity, ticks=False
        )
        .configure_view(strokeOpacity=stroke_opacity)
    )

    base2 = (
        alt.Chart(df)
        .mark_line()
        .encode(
            x=alt.X(
                "Date",
                timeUnit=time_unit,
                title="date",
                axis=alt.Axis(format=monthdate_format),
            ),
            y=alt.Y("n_available", title="# available"),
            color=alt.Color(
                "Type",
                scale=alt.Scale(
                    domain=["Acute Care Beds", "ICU Beds", "Ventilators"],
                    range=[acute_color, icu_color, ventilator_color],
                ),
            ),
        )
    )

    hospital_num_chart = (
        base2.properties(
            title="Number of Available Hospital Equipment by Type", width=chart_width
        )
        .configure_title(
            fontSize=title_font_size, font=font_name, anchor="middle", color="black"
        )
        .configure_axis(
            gridOpacity=grid_opacity, domainOpacity=domain_opacity, ticks=False
        )
        .configure_view(strokeOpacity=stroke_opacity)
    )

    display(hospital_pct_chart)
    display(hospital_num_chart)