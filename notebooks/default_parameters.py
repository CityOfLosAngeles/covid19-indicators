import pandas as pd
import pytz
from datetime import date, datetime, timedelta

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
    ).strftime(fulldate_format)
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

two_days_ago = (
    (datetime.today()
                .astimezone(pytz.timezone(f'{time_zone}'))
                .date()
        - timedelta(days=2)
    ).strftime(fulldate_format)
)