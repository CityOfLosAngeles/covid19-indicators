import pandas as pd
import pytz
from datetime import date, datetime, timedelta
import os
import s3fs

def remap_missing_file (PATH1,PATH2,Filename):
    fs=s3fs.S3FileSystem(anon=False)
    tmp_path=PATH1
    if not fs.exists(tmp_path+Filename):
        tmp_path=PATH2
    return tmp_path+Filename

bucket_name = "public-health-dashboard"
env_list=dict(os.environ)
if "CURRENT_BRANCH" in env_list:
    CURRENT_BRANCH = os.environ["CURRENT_BRANCH"]
    search_str=CURRENT_BRANCH + "_env_S3_FILE_PATH"
    if search_str in env_list:
        S3_FILE_PATH = os.environ[search_str]
    else:
        S3_FILE_PATH = f"s3://{bucket_name}/Test/jhu_covid19/"

    search_str=CURRENT_BRANCH + "_env_S3_FILE_PATH_SOURCE"
    if search_str in env_list:
        S3_FILE_PATH_SOURCE = os.environ[search_str]
    else:
        S3_FILE_PATH_SOURCE = f"s3://{bucket_name}/jhu_covid19/"
else:
    CURRENT_BRANCH="test"

county_state_name = "Los Angeles, CA"
state_name = "California"
msa_name = "Los Angeles-Long Beach-Anaheim, CA"

fulldate_format = "%-m/%-d/%y"
monthdate_format = "%-m/%-d"
time_zone = "US/Pacific"
start_date = datetime(2020, 4, 15).date()

yesterday_date = (
    (datetime.today()
                .astimezone(pytz.timezone(f'{time_zone}'))
                .date()
        - timedelta(days=1)
    )
)

today_date = (
    datetime.today()
             .astimezone(pytz.timezone(f'{time_zone}'))
             .date()
)

one_week_ago = (
    (datetime.today()
                .astimezone(pytz.timezone(f'{time_zone}'))
                .date()
        - timedelta(days=8)
    )
) 

two_weeks_ago = (
    (datetime.today()
                .astimezone(pytz.timezone(f'{time_zone}'))
                .date()
        - timedelta(days=15)
    )
)

three_weeks_ago = (
    (datetime.today()
                .astimezone(pytz.timezone(f'{time_zone}'))
                .date()
        - timedelta(days=22)
    )
)

two_days_ago = (
    (datetime.today()
                .astimezone(pytz.timezone(f'{time_zone}'))
                .date()
        - timedelta(days=2)
    )
)

three_days_ago = (
    (datetime.today()
                .astimezone(pytz.timezone(f'{time_zone}'))
                .date()
        - timedelta(days=3)
    )
)

eight_days_ago = (
    (datetime.today()
                .astimezone(pytz.timezone(f'{time_zone}'))
                .date()
        - timedelta(days=8)
    )
)

nine_days_ago = (
    (datetime.today()
                .astimezone(pytz.timezone(f'{time_zone}'))
                .date()
        - timedelta(days=9)
    )
)

one_month_ago = (
    (datetime.today()
                .astimezone(pytz.timezone(f'{time_zone}'))
                .date()
        - timedelta(days=31)
    )
)
