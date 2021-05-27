"""
Simplified functions to use in test-report.

Source credentials: https://stackoverflow.com/questions/61816351/how-to-get-a-csv-into-a-dataframe-from-gcs-with-credentials-from-script

This didn't really work?
https://stackoverflow.com/questions/56596951/save-pandas-data-frame-to-google-cloud-bucket
"""

import gcsfs
import pandas as pd

CREDENTIAL = "../gcp-credential.json"
gs = gcsfs.GCSFileSystem(project="ita-datalakepoc") #token=f"{CREDENTIAL}"

BUCKET_NAME = "electedoffice_covid19_indicators"

COUNTY_VACCINE_URL = (
    "https://data.chhs.ca.gov/dataset/e283ee5a-cf18-4f20-a92c-ee94a2866ccd/resource/"
    "130d7ba2-b6eb-438d-a412-741bde207e1c/download/"
    "covid19vaccinesbycounty.csv"
)

#---------------------------------------------------------------#
# Vaccines Administered
#---------------------------------------------------------------#
def clean_vaccines_by_county():
    df = pd.read_csv(COUNTY_VACCINE_URL)
    
    POP_URL = "https://raw.githubusercontent.com/CityOfLosAngeles/covid19-indicators/master/data/ca_county_pop_crosswalk.csv"
    population = pd.read_csv(POP_URL, dtype={"county_fips": "str"})    
    
    df = pd.merge(df, population, 
                  on = "county",
                  how = "inner", validate = "m:1")
    
    df = df.assign(
        date = pd.to_datetime(df.administered_date),
    )
    
    # Reshape and make long
    id_vars = ["county", "administered_date", "date", 
               "county_fips", "county_pop2020", "california_flag"]
    
    df2 = df.melt(id_vars=id_vars)
    
    # Let's also get the proportion relative to that county's pop
    # Ultimately, only interested in partially/fully vaccinated population,
    # but generate it for all the other variables too
    df2 = df2.assign(
        proportion = df2.value.divide(df2.county_pop2020)
    )
    
    df2.to_parquet(f"gcs://{BUCKET_NAME}/vaccines_by_county.parquet")  
    
    return df2
