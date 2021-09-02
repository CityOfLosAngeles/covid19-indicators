import dotenv
import gcsfs
import os
import pandas as pd

from processing_utils import default_parameters
from processing_utils import neighborhood_utils
from processing_utils import utils
from processing_utils import socrata_utils

S3_FILE_PATH = default_parameters.S3_FILE_PATH

dotenv.load_dotenv()

SOCRATA_USER = os.environ["SOCRATA_USERNAME"]
SOCRATA_PASSWORD = os.environ["SOCRATA_PASSWORD"]

CREDENTIAL = "../gcp-credential.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = f'{CREDENTIAL}'

gs = gcsfs.GCSFileSystem(project="ita-datalakepoc")
GCS_BUCKET_NAME = "electedoffice_vaccines_dashboard"


DATAFRAME_DICT = {
    #key: str, socrata_dataset_id
    #value: str, csv_file file path
    "jsff-uc6b": "us-county-time-series.csv",
    "fvye-93wd": "la-county-neighborhood-time-series.csv",
    "rpp7-mevy": "vaccinations-by-county.csv",
    "iv7a-6rrq": "vaccinations-by-demographics-county.csv",
    "w9vh-pj9e": "la-county-testing-time-series.csv",
    "5xru-6ubk": "la-city-vaccines.csv",
}

def us_county(csv_file, county_list=["Los Angeles"]):
    parquet_file = csv_file.replace('.csv', '.parquet')
    
    df = pd.read_parquet(f"{S3_FILE_PATH}{parquet_file}")
 
    df = (df[df.county.isin(county_list)]
          .reset_index(drop=True)
          .assign(
            date = pd.to_datetime(df.date).dt.date,
            incident_rate = df.incident_rate.round(1),
        )
    )
    
    # Subsetting by date needs to happen after we manipulate date column
    # Otherwise, datetime.date clashes with pandas.timestamp
    df = df[df.date < default_parameters.today_date]
    
    return df


def la_county_neighborhood(csv_file):
    df = neighborhood_utils.clean_data()

    keep_cols = [
        "aggregate_region", "population", "date", "date2",
        "cases", "deaths", "new_cases", "new_deaths",
        "cases_per100k", "deaths_per100k",
        "cases_avg7", "deaths_avg7",
        "new_cases_avg7", "new_deaths_avg7",
        "cases_per100k_avg7", "deaths_per100k_avg7",
    ]
    
    df = df[keep_cols]
    
    # Round some of the other columns to 1 decimal place
    integrify_me = ["cases", "deaths", 
                    "new_cases", "new_deaths", "population"]

    round_me = [c for c in df.columns if (c not in ["aggregate_region", "date", "date2"])
               and (c not in integrify_me)]

    df[round_me] = df[round_me].astype(float).round(1)
    df[integrify_me] = df[integrify_me].astype(int)

    return df


def ca_vaccinations(csv_file):
    df = pd.read_csv(utils.COUNTY_VACCINE_URL)

    population = pd.read_parquet(f"{S3_FILE_PATH}ca_county_pop_crosswalk.parquet")    

    df = pd.merge(df, population, 
                  on = "county",
                  how = "inner", validate = "m:1")

    df = df.assign(
        date = pd.to_datetime(df.administered_date),
    ).drop(columns = ["administered_date"])
        
    return df

def ca_vaccinations_demographics(csv_file):
    df = pd.read_csv(utils.COUNTY_DEMOGRAPHICS_URL)
    
    df = df.assign(
        date = pd.to_datetime(df.administered_date),
    ).drop(columns = ["administered_date"])
    
    string_cols = ["county", "county_type", 
                   "demographic_category", "demographic_value",
                   "date", "suppress_data"]
    
    integrify_me = [c for c in df.columns if c not in string_cols]
    df[integrify_me] = df[integrify_me].astype("Int64")
    
    return df

def la_county_testing(csv_file):
    s3_file_name = "county-city-testing.parquet" 
    
    df = pd.read_parquet(f"{S3_FILE_PATH}{s3_file_name}")
    df = df.assign(
        date = pd.to_datetime(df.date).dt.date,
    )
    
    # Drop the row that has all 0's, even if it's yesterday
    # Keep data ending in 2 days ago
    df = df[df.date <= default_parameters.two_days_ago]
    
    return df


def city_of_la_vaccinations(csv_file):
    file_name = csv_file.replace('-', '_')
    
    df = pd.read_csv(f"gcs://{GCS_BUCKET_NAME}/data/{file_name}", 
                     dtype={"patient_zipcode": "str"})  
    
    # Aggregate data for Socrata
    group_cols = [
        "appointment_date",
        "site_type", "agegroup", "race_ethnicity", 
        "patient_race", "patient_ethnicity", "patient_gender", 
        "vaccine_site_name", "shot_number", "brand", "appt_status",
    ]

    '''
    NOTE: appointment_date and apptdate are both present
    appointment_date may be when the patient signed up?
    apptdate is date of physical appt? 
    Double check. Also, correct the 1972 errors in apptdate, 
    We know this must be true: appointment_date <= apptdate, but there's hundreds of thousands that fail this condition
    '''
 
    df2 = (df.groupby(group_cols)
           .agg({"vaccine_id": "count"})
           .reset_index()
           .rename(columns = {
               "appointment_date": "date",
               "vaccine_id": "num_vaccines"})
          )
    
    return df2
    

def extra_processing(csv_file):
    if csv_file=="us-county-time-series.csv":
        df = us_county(csv_file, county_list=["Los Angeles"])
    elif csv_file=="la-county-neighborhood-time-series.csv":
        df = la_county_neighborhood(csv_file)
    elif csv_file=="vaccinations-by-county.csv":
        df = ca_vaccinations(csv_file)
    elif csv_file=="vaccinations-by-demographics-county.csv": 
        df = ca_vaccinations_demographics(csv_file) 
    elif csv_file=="vaccinations-by-demographics-county.csv": 
        df = ca_vaccinations_demographics(csv_file)
    elif csv_file=="la-county-testing-time-series.csv": 
        df = la_county_testing(csv_file)
    elif csv_file=="la-city-vaccines.csv":
        df = city_of_la_vaccinations(csv_file)
    else:
        df = pd.read_csv(f"{S3_FILE_PATH}{csv_file}")
    
    return df


for socrata_id, filename in DATAFRAME_DICT.items():
    # So far, all the datasets need some extra processing to make sure Socrata table schema is correct
    df = extra_processing(filename)
    
    # Write the full table out as CSV 
    df.to_csv(f"{filename}", index=False)
    print(f"{filename} produced")
    
    #socrata_utils.overwrite_socrata_table(SOCRATA_USER, SOCRATA_PASSWORD, 
    #                                      filename, socrata_dataset_id = socrata_id)
    socrata_utils.upsert_socrata_rows(SOCRATA_USER, SOCRATA_PASSWORD, 
                                      filename, socrata_dataset_id = socrata_id)
    