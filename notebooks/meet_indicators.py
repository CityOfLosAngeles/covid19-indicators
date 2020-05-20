import utils

time_zone = utils.time_zone
start_date = utils.start_date
yesterday_date = utils.yesterday_date
today_date = utils.today_date
two_weeks_ago = utils.two_weeks_ago


def meet_county_case(county_state_name, start_date):
    df = utils.county_case_indicators(county_state_name, start_date)
    df = df[(df.date < today_date) & (df.date >= two_weeks_ago)]
    df = county_past_two_weeks(df)
    
    county_case_indicator = df.iloc[0]["days_fewer_cases"]
    
    return county_case_indicator


def meet_county_death(county_state_name, start_date):
    df = utils.county_case_indicators(county_state_name, start_date)
    df = df[(df.date < today_date) & (df.date >= two_weeks_ago)]
    df = county_past_two_weeks(df)
    
    county_death_indicator = df.iloc[0]["days_fewer_deaths"]
    
    return county_death_indicator


def meet_lacity_case(start_date):
    df = utils.lacity_case_indicators(start_date)
    df = df[(df.date < today_date) & (df.date >= two_weeks_ago)]
    df = lacity_past_two_weeks(df)
    
    lacity_case_indicator = df.iloc[0]["days_fewer_cases"]
    
    return lacity_case_indicator


# Daily Testing
def meet_daily_testing(yesterday_date, test_lower_bound, test_upper_bound):
    """
    Returns red/green/blue depending on how well benchmark is met
    """
    df = utils.lacity_testing_indicators(start_date, test_lower_bound, test_upper_bound)
    
    test_indicator = df[df.date==yesterday_date].iloc[0]["Performed"]
    print(test_indicator)
    if test_indicator < test_lower_bound:
        return "red"

    elif (test_indicator >= test_lower_bound) and (test_indicator < test_upper_bound):
        return "green"

    elif test_indicator >= test_upper_bound:
        return "blue"
    
    else:
        return "white"

    
# Share of Positive Results
def meet_positive_share(yesterday_date, positive_bound):
    """
    Returns red/green depending on if benchmark was met last week
    """
    df = utils.lacity_positive_test_indicators(start_date)
    
    positive_indicator = df[df.week == df.week.max()].iloc[0]["pct_positive"]
    
    if positive_indicator < positive_bound:
        return "green"
    
    elif positive_indicator >= positive_bound:
        return "red"

    
# Hospital Equipment
def meet_hospital(yesterday_date, hospital_bound):
    """
    Returns red/green depending on if benchmark was net yesterday
    """
    df = utils.lacity_hospital_indicators(start_date)
    
    positive_indicator = df[df.week == df.week.max()].iloc[0]["pct_positive"]
    
    if positive_indicator < positive_bound:
        return "green"
    
    elif positive_indicator >= positive_bound:
        return "red"    
    
   
"""
Sub-functions
"""
def county_past_two_weeks(df):
    """
    Count number of times in past 14 days where
    we had drop in cases / deaths from prior day.
    We will use 7-day rolling average for BOTH cases and deaths.
    Date two weeks ago is 15 days ago because
    we need 14 change-from-prior-day observations.
    """
    sort_cols = ["date"]
    group_cols = ["county", "state"]
    
    df = df.assign(
        delta_cases_avg7=(
            df.sort_values(sort_cols)
            .groupby(group_cols)["cases_avg7"]
            .diff(periods=1)
        ),
        delta_deaths_avg7=(
            df.sort_values(sort_cols)
            .groupby(group_cols)["deaths_avg7"]
            .diff(periods=1)
        )
    )

    df = df.assign(
        days_fewer_cases = df.apply(lambda row: 1 if row.delta_cases_avg7 < 0 else 0, axis=1),
        days_fewer_deaths = df.apply(lambda row: 1 if row.delta_deaths_avg7 < 0 else 0, axis=1),
    )

    two_week_totals = (df.groupby(["county", "fips"])
                        .agg({"days_fewer_cases": "sum", 
                            "days_fewer_deaths": "sum"})
                        .reset_index()
                        )

    return two_week_totals
    

def lacity_past_two_weeks(df):
    sort_cols = ["date"]
    
    df = df.assign(
        delta_cases_avg7=(
            df.sort_values(sort_cols)["cases_avg7"]
            .diff(periods=1)
        ),
    )

    df = df.assign(
        days_fewer_cases = df.apply(lambda row: 1 if row.delta_cases_avg7 < 0 else 0, axis=1),
    )

    two_week_totals = (df.agg({"days_fewer_cases": "sum"})
                        .reset_index()
                    )

    return two_week_totals