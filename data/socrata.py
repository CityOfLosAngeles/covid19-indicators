import dotenv
import os
import pandas as pd

from arcgis.gis import GIS
from arcgis.features import FeatureLayerCollection, FeatureLayer

from processing_utils import default_parameters
from processing_utils import neighborhood_utils
from processing_utils import utils
from processing_utils import socrata_utils

S3_FILE_PATH = default_parameters.S3_FILE_PATH

dotenv.load_dotenv()

SOCRATA_USER = os.environ["SOCRATA_USERNAME"]
SOCRATA_PASSWORD = os.environ["SOCRATA_PASSWORD"]

LAHUB_USER = os.environ["LAHUB_USER"]
LAHUB_PASSWORD = os.environ["LAHUB_PASSWORD"]

DATAFRAME_DICT = {
    #key: str, socrata_dataset_id
    #value: str, csv_file file path
    "jsff-uc6b": "us-county-time-series.csv",
    "fvye-93wd": "la-county-neighborhood-time-series.csv",
    "rpp7-mevy": "vaccinations-by-county.csv",
    "iv7a-6rrq": "vaccinations-by-demographics-county.csv",
    "w9vh-pj9e": "la-county-testing-time-series.csv",
    "7yet-b6aj": "la-county-neighborhood-vaccination.csv",
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


def la_neighborhood_vax(csv_file):
    #ITEM_URL = "https://lahub.maps.arcgis.com/home/item.html?id=10532e9971e647caadfd69ecbffc4c74"
    #SERVICE_URL "https://services5.arcgis.com/VAb1qw880ksyBtIL/arcgis/rest/services/Los_Angeles_COVID_Cases_and_Vaccinations/FeatureServer/0"

    geohubUrl="https://lahub.maps.arcgis.com"
    FEATURE_LAYER_ID = "10532e9971e647caadfd69ecbffc4c74"
    geohub = GIS(geohubUrl, LAHUB_USER, LAHUB_PASSWORD)
    
    flayer = geohub.content.get(FEATURE_LAYER_ID)
    
    # Can't get feature layer collection
    # Will query and return the spatial df instead
    # Table is not time-series, it's the most up-to-date info, based on when it was last downloaded
    sdf = flayer.layers[0].query().sdf
    
    keep_cols = [
        "CITY_TYPE", "LCITY", "COMMUNITY", "LABEL", 
        "SOURCE", "City_Community", 
        "Cases", "Case_Rate", "Deaths", "Death_Rate", 
        'F16__with_1__Dose', 'F16__Pop__Vaccinated____', 'F65__with_1__Dose',
        'F65__Pop__Vaccinated____',
    ]

    sdf2 = (sdf[keep_cols]
            .rename(columns = {
                'F16__with_1__Dose': 'pop16+_atleast1dose', 
                'F16__Pop__Vaccinated____': 'pop16+_pct_partialvax',
                'F65__with_1__Dose': 'pop65+_atleast1dose',
                'F65__Pop__Vaccinated____': 'pop65+_pct_partialvax',
                'LCITY': 'CITY',
            })
           )
    
    return sdf2


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
    elif csv_file=="la-county-neighborhood-vaccination.csv":
        df = la_neighborhood_vax(csv_file)
    else:
        df = pd.read_csv(f"{S3_FILE_PATH}{csv_file}")
    
    return df


for socrata_id, filename in DATAFRAME_DICT.items():
    # So far, all the datasets need some extra processing to make sure Socrata table schema is correct
    df = extra_processing(filename)
    
    # Write the full table out as CSV 
    df.to_csv(f"{filename}", index=False)
    print(f"{filename} produced")
    
    if filename == "la-county-neighborhood-vaccination.csv":
        socrata_utils.overwrite_socrata_table(SOCRATA_USER, SOCRATA_PASSWORD, 
                                              filename, socrata_dataset_id = socrata_id)
    else:
        socrata_utils.upsert_socrata_rows(SOCRATA_USER, SOCRATA_PASSWORD, 
                                          filename, socrata_dataset_id = socrata_id)
    