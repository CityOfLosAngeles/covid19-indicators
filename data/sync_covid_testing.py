"""
Pull data from MOPS COVID Dashboard and upload to S3
"""
import datetime
import os
import pandas as pd
import pytz

bucket_name = "public-health-dashboard"
S3_FILE_PATH = f"s3://{bucket_name}/jhu_covid19/"

filename = "COVID_testing_data.csv"
workbook = (
    "https://docs.google.com/spreadsheets/d/"
    "1agPpAJ5VNqpY50u9RhcPOu7P54AS0NUZhvA2Elmp2m4/"
    "export?format=xlsx&#gid=0"
)
sheet_name = "DUPLICATE OF MOPS"


def get_county_data(filename, workbook, sheet_name):
    df = pd.read_excel(workbook, sheet_name=sheet_name, skiprows=1, index_col=0)
    df = df.T

    select_rows = [1, 2, 3, 
                7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 
                25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46 
                ]
    df = df.iloc[:, select_rows].reset_index(level=0)

    column_names = [
        "Date",
        "City_Subtotal",
        "County_Subtotal",
        "Total_Performed",
        "Dodgers_Stadium",
        "Hansen_Dam",
        "Crenshaw_Christian",
        "Lincoln_Park",
        "West_Valley_Warner",
        "Carbon_Health_Echo_Park",
        "Kedren_Health",
        "Special_LAPD_LAFD",
        "HACLA",
        "Nursing_Home_Outreach",
        "Homeless_Outreach",
        "Hotchkin_Elysian_Park",
        "VA_Lot15_Jackie_Robinson",
        "Baldwin_Hills_Crenshaw",
        "Northridge_Fashion",
        "Pomona_Fairplex",
        "Redondo_Beach_District",
        "Palmdale",
        "Long_Beach_CC",
        "Charles_Drew_Campus",
        "Santa_Clarita",
        "East_LA_Civic",
        "The_Forum",
        "Bellflower_Civic",
        "San_Gabriel_Valley_Airport",
        "High_Desert_Lancaster",
        "Northridge_Hospital_Medical",
        "AltaMed",
        "Cedars_Sinai",
        "City_of_Bell",
        "Glendale_Memorial",
        "Beverly_Hospital_Montebello",
        "Whittier_PIH",
        "Good_Samaritan_LA",
        "Harbor_UCLA_Medical",
        "AVORS_Medical_Lancaster",
        "Pasadena_Rose_Bowl",
    ]

    df.columns = column_names

    # Drop the Total column
    df = df[df.Date != "Total"]

    df = (df.assign(
        Date = pd.to_datetime(df.Date),
        ).sort_values("Date")
        .reset_index(drop=True)
    )

    # Fill in missing values with 0's for city sites
    city_sites = [
        "Dodgers_Stadium",
        "Hansen_Dam",
        "Crenshaw_Christian",
        "Lincoln_Park",
        "West_Valley_Warner",
        "Carbon_Health_Echo_Park",
        "Kedren_Health",
        "Special_LAPD_LAFD",
        "HACLA",
        "Nursing_Home_Outreach",
        "Homeless_Outreach",
        "Hotchkin_Elysian_Park",
        "VA_Lot15_Jackie_Robinson",
        "Baldwin_Hills_Crenshaw",
        "Northridge_Fashion",
    ]

    county_sites = [
        "Pomona_Fairplex",
        "Redondo_Beach_District",
        "Palmdale",
        "Long_Beach_CC",
        "Charles_Drew_Campus",
        "Santa_Clarita",
        "East_LA_Civic",
        "The_Forum",
        "Bellflower_Civic",
        "San_Gabriel_Valley_Airport",
        "High_Desert_Lancaster",
        "Northridge_Hospital_Medical",
        "AltaMed",
        "Cedars_Sinai",
        "City_of_Bell",
        "Glendale_Memorial",
        "Beverly_Hospital_Montebello",
        "Whittier_PIH",
        "Good_Samaritan_LA",
        "Harbor_UCLA_Medical",
        "AVORS_Medical_Lancaster",
        "Pasadena_Rose_Bowl",
    ]

    # Sometimes a TBD or unknown is found. Coerce to be NaNs until it is filled in later.
    replace_me = ["TBD", "tbd", "unknown", "Unknown", "UNKNOWN", "unk"]

    def replace_str_with_zero(df, cols_to_apply):
        df[cols_to_apply] = df[cols_to_apply].replace(replace_me, 0)
        df[cols_to_apply] = df[cols_to_apply].fillna(0).astype(int)
        
        return df
    
    total_cols = ["City_Subtotal", "County_Subtotal", "Total_Performed"]

    df = replace_str_with_zero(df, city_sites)
    df = replace_str_with_zero(df, county_sites)
    df = replace_str_with_zero(df, total_cols)

    df = (df.assign(
            City_Subtotal=df.City_Subtotal.astype(int),
            County_Subtotal=df.County_Subtotal.astype(int),
            Total_Performed=df.Total_Performed.astype(int),
        ).rename(columns = {
            "City_Subtotal":"City_Performed",
            "County_Subtotal": "OtherCounty_Performed",
            "Total_Performed": "Performed"
        })
    )
    
    df = (df.sort_values("Date")
            .reset_index(drop=True)
    )

    df.to_parquet(f"{S3_FILE_PATH}county-city-testing-sites.parquet")
    
    return df

        
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
# LA County RShiny output
def update_covid_testing_from_rshiny():
    df = pd.read_csv(f"{S3_FILE_PATH}county-persons-tested-rshiny.csv")
    
    keep = ["date", "tests", "tests_pos"]

    df = (df.assign(
            date = pd.to_datetime(df.date_use),
        ).rename(columns = {"persons_tested_all": "tests",
                           "positive_persons_all": "tests_pos"})
          .sort_values("date")
          .reset_index(drop=True)
        [keep]  
    )

    df.to_parquet(f"{S3_FILE_PATH}county-persons-tested-rshiny.parquet")
    
    return df
    
"""
Add a new column to the file we already put in S3.
Just in case Mayor's office updates resume for county,
don't want to lose the distinction between city/county sites
and totals including private healthcare facilities.
"""
def update_covid_testing_city_county_data(**kwargs):
    city_county_df = get_county_data(filename, workbook, sheet_name)
    county_rshiny_df = update_covid_testing_from_rshiny()
    
    # Clean up columns before merging
    keep = ["Date", "City_Performed", "Performed"]
    city_county_df = (city_county_df[keep]
                      .rename(columns = {"Date": "date",
                                         "City_Performed": "City_Site_Performed", 
                                         "Performed": "County_Site_Performed"})
                     )

    # County has the number of positive for each test batch
    county_rshiny_df = county_rshiny_df.rename(columns = {"tests": "County_Performed", 
                                                         "tests_pos": "County_Positive"})
    
    # Merge together
    # County's data goes back to 3/10, earlier than Mayor's spreadsheet, which is 3/20
    df = pd.merge(county_rshiny_df, city_county_df, 
                  on = "date", how = "left", validate = "1:1")
    
    integrify_me = ["County_Performed", "County_Positive", 
                    "City_Site_Performed", "County_Site_Performed"]
    
    df[integrify_me] = df[integrify_me].astype("Int64")
    
    # Export to S3
    df.to_csv(f"{S3_FILE_PATH}county-city-testing.csv", index=False)
    df.to_parquet(f"{S3_FILE_PATH}county-city-testing.parquet")

