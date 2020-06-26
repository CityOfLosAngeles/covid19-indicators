"""
Pull data from CA open data related to medical surge facilities 
and hospital data
"""
import intake
import intake_dcat
import os
import pandas as pd

catalog = intake.open_catalog("../catalog.yml")
bucket_name = "public-health-dashboard"

def clean_surge_data(df):
    keep = ["county", "date", "type_of_facility", "available_beds", "occupied_beds"] 

    df = (df[df.status=="Ready"]
          .assign(
              date = pd.to_datetime(df.date),
              available_beds = df.beds_ready_to_accept_patients.astype("Int64"),
              occupied_beds = df.patients_in_beds.astype("Int64"),
          )[keep]
         )
    
    # Get county total available / occupied beds
    group_cols = ["county", "date"]
    for col in ["available_beds", "occupied_beds"]:
        new_col = f"county_{col}"
        df[new_col] = df.groupby(group_cols)[col].transform("sum")
    
    return df


def clean_hospital_data(df):
    df = (df.assign(
            date = pd.to_datetime(df.todays_date),
        ).rename(
        columns = {
            "previous_days_covid_confirmed_patients": "prior_confirmed",
            "previous_days_suspected_covid_patients": "prior_suspected",
            "previous_days_conversion_covid_patients": "prior_conversion",
            "hospitalized_covid_confirmed_patients": "hospitalized_confirmed",
            "hospitalized_suspected_covid_patients": "hospitalized_suspected",
            "hospitalized_covid_patients": "hospitalized_covid",
            "icu_covid_confirmed_patients": "icu_confirmed",
            "icu_suspected_covid_patients": "icu_suspected", 
            "icu_available_beds": "icu_available",
        }).drop(columns = ["todays_date"])
    )
    
    integrify_me = ["prior_confirmed", "prior_suspected", "prior_conversion", 
                    "hospitalized_confirmed", "hospitalized_suspected", 
                    "hospitalized_covid", "all_hospital_beds", 
                    "icu_confirmed", "icu_suspected", "icu_available"]
    
    df[integrify_me] = df[integrify_me].astype("Int64")
    
    df = df.assign(
        hospitalized_covid = df.hospitalized_confirmed + df.hospitalized_suspected,
        icu_covid = df.icu_confirmed + df.icu_suspected
    )

    col_order = [
        "county", "county_fips", "date", 
        "prior_confirmed", "prior_suspected", "prior_conversion", 
        "hospitalized_confirmed", "hospitalized_suspected", 
        "hospitalized_covid", "all_hospital_beds", 
        "icu_confirmed", "icu_suspected", "icu_covid", "icu_available"
    ]

    # There are some dates that are NaT...drop these
    # Note: there are obs that have NaT but only have all_hospital_beds filled....
    # If there is no data, let's exclude for now, instead of interpolating and assuming.
    df = (df[df.date.notna()]
          .sort_values(["county", "date"])
          .reindex(columns = col_order)
          .reset_index(drop=True)
         )
    
    return df


def grab_county_fips(df):
    crosswalk = pd.read_csv('../data/msa_county_pop_crosswalk.csv', dtype = {"county_fips":"str"})
    
    keep = ["county", "county_fips"]
    crosswalk = (crosswalk[crosswalk.state == "California"][keep]
                    .assign(county = crosswalk.county.str.replace(" County", ""))
            )
    
    df = pd.merge(df, crosswalk, on = "county", how = "left", validate = "m:1")

    # Reorder county_fips to be right after county
    cols_to_order = ["county", "county_fips", "date"]
    new_col_order = cols_to_order + (df.columns.drop(cols_to_order).tolist())
    df = df[new_col_order]

    return df
    


def update_ca_surge_hospital_data(**kwargs):    
    # Grab hospital capacity data
    hospital_df = catalog.ca_open_data.hospital_capacity.read()
    hospital_df = clean_hospital_data(hospital_df)
    hospital_df = grab_county_fips(hospital_df)

    # Grab surge capacity data
    surge_df = catalog.ca_open_data.medical_surge_facilities.read()
    surge_df = clean_surge_data(surge_df)
    surge_df = grab_county_fips(surge_df)
    
    # Export to S3 separately
    hospital_df.to_parquet(f"s3://{bucket_name}/jhu_covid19/ca-hospital-capacity.parquet")
    surge_df.to_parquet(f"s3://{bucket_name}/jhu_covid19/ca-medical-surge-facilities.parquet")

    """
    Create a county time-series df at county-date level.
    Show: hospitalized covid, ICU covid, pct of hospital beds occupied, 
    percent of ICU beds occupied, and number of beds available for surge.

    Hospital data is at the county-date level.
    Problem with hospital data is that there are NaN for county, yet there are covid patients.
    Also, there are not always all_hospital_beds available.

    Surge data is at the county-facility type-date level.
    Problem with surge data is that it doesn't always report occupied beds...
    we're interested in available beds anyway.
    Also, not sure how surge facilities match with hospitals...surge facilities include hospital
    as a facility type, so likely it's a subset.
    """
    
    keep_surge = ["date", "county", "county_fips", 
              "county_available_beds"]
    surge_df2 = surge_df[keep_surge].drop_duplicates()

    # Not sure what is going on in 
    keep_hospital = ["date", "county", "county_fips", 
                "hospitalized_covid", "all_hospital_beds", "icu_covid", "all_icu_beds"]

    hospital_df2 = hospital_df[hospital_df.county.notna()][keep_hospital].drop_duplicates()

    # Merge together
    m1 = pd.merge(hospital_df2, surge_df2, 
            on = ["county_fips", "county", "date"], how = "left", validate = "1:1")

    # Due to data problems or lack of understanding dataset, hold off on calculating
    # percent of available hospital beds with this.
    # Let's just plot hospitalized COVID patients and ICU patients (subset of all hospitalized)
    m1 = (m1.rename(columns = {
        "county_available_beds": "surge_available_beds"
        })
        .sort_values(["county_fips", "date"])
        .reset_index(drop=True)
    )

    m1.to_parquet(f"s3://{bucket_name}/jhu_covid19/ca-hospital-and-surge-capacity.parquet")
