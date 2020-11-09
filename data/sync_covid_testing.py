"""
Pull data from MOPS COVID Dashboard and upload to S3
"""
import datetime
import os
import pandas as pd
import pytz

bucket_name = "public-health-dashboard"
S3_FILE_PATH = f"s3://{bucket_name}/jhu_covid19/"

        
"""
Mayor's office no longer updating county test totals.
Always putting unknown or TBD in the spreadsheet.
Use LA County's RShiny dashboard instead - PERSONS TESTED BY DATE.
These report county totals, which should include private healthcare providers.
But, we don't know which subset is the City's, so we'll use the 
Mayor's spreadsheet for that, which is a subset of actual tests conducted 
by the City, since these are tests done at City sites, not private healthcare
facilities within City boundaries.
"""
# LA County Persons Tested RShiny table
def update_persons_tested_rshiny():
    df = pd.read_csv(f"{S3_FILE_PATH}county-persons-tested-rshiny.csv")
    
    keep = ["date", "persons_tested", "persons_pos"]

    df = (df.assign(
            date = pd.to_datetime(df.date_use),
        ).rename(columns = {"persons_tested_all": "persons_tested",
                           "positive_persons": "persons_pos"})
          .sort_values("date")
          .reset_index(drop=True)
        [keep]  
    )

    df.to_parquet(f"{S3_FILE_PATH}county-persons-tested-rshiny.parquet")
    
    return df

# LA County Tests Performed RShiny table
def update_tests_performed_rshiny():
    df = pd.read_csv(f"{S3_FILE_PATH}county-tests-performed-rshiny.csv")
    
    keep = ["date", "tests", "tests_pos"]

    df = (df.assign(
            date = pd.to_datetime(df.date_use),
        )
          .sort_values("date")
          .reset_index(drop=True)
        [keep]  
    )

    df.to_parquet(f"{S3_FILE_PATH}county-tests-performed-rshiny.parquet")
    
    return df

    
"""
Add a new column to the file we already put in S3.
Just in case Mayor's office updates resume for county,
don't want to lose the distinction between city/county sites
and totals including private healthcare facilities.
"""
def update_covid_testing_city_county_data(**kwargs):
    persons_tested_df = update_persons_tested_rshiny()
    tests_performed_df = update_tests_performed_rshiny()


    # County has the number of positive for each test batch
    persons_tested_df = persons_tested_df.rename(
                        columns = {"persons_tested": "County_Person_Performed", 
                                    "persons_pos": "County_Person_Positive"})

    tests_performed_df = tests_performed_df.rename(
                        columns = {"tests": "County_Performed", 
                                    "tests_pos": "County_Positive"})
    
    # Merge together
    # County's data goes back to 3/10, earlier than Mayor's spreadsheet, which is 3/20
    df = pd.merge(persons_tested_df, tests_performed_df, 
                    on = "date", how = "left", validate = "1:1")

    
    integrify_me = ["County_Person_Performed", "County_Person_Positive",
                    "County_Performed", "County_Positive",
                   ]
    
    df[integrify_me] = df[integrify_me].astype("Int64")
    
    # Export to S3
    df.to_csv(f"{S3_FILE_PATH}county-city-testing.csv", index=False)
    df.to_parquet(f"{S3_FILE_PATH}county-city-testing.parquet")

