"""
Pull data from CA open data related to PPE 
"""
import intake
import intake_dcat
import os
import pandas as pd

# Civis container script clones the repo and we are in /app
# Feed it the absolute path to our catalog.yml
catalog = intake.open_catalog("/app/catalog.yml")
bucket_name = "public-health-dashboard"

def clean_data(df):
    df = (df.assign(
            quantity_filled = df.quantity_filled.astype("Int64"),
            date = pd.to_datetime(df.as_of_date),
            zip_code = df.shipping_zip_postal_code,
        ).drop(columns = ["as_of_date", "shipping_zip_postal_code"])
    )
    
    # Feed Civis the absolute path to our crosswalk, which must start with /app
    crosswalk = pd.read_csv('/app/data/msa_county_pop_crosswalk.csv', dtype = {"county_fips":"str"})
    
    keep = ["county", "county_fips"]
    crosswalk = (crosswalk[crosswalk.state == "California"][keep]
                    .assign(county = crosswalk.county.str.replace(" County", ""))
            )
    
    df = pd.merge(df, crosswalk, on = "county", how = "left", validate = "m:1")
    
    # Tag the groups of PPE
    tagged = df.apply(categorize_ppe, axis=1)
    df = pd.concat([df, tagged], axis=1)
    
    # Get everything down to county-zipcode-date level
    keep_cols = ["county", "county_fips", "zip_code", "date", 
                 "n95", "surgical_masks", "cloth_masks", 
                 "eye_protection", "face_shields", "gloves", "gowns"]

    group_cols = ["county", "county_fips", "zip_code", "date"]

    ppe_items = ["n95", "surgical_masks", "cloth_masks", 
                 "eye_protection", "face_shields", "gloves", "gowns"]

    for col in ppe_items:
        df[col] = df.groupby(group_cols)[col].transform("max").astype("Int64")


    df = (df[keep_cols]
           .drop_duplicates()
           .sort_values(group_cols)
           .reset_index(drop=True)
    )

    # Also get county-date totals for the PPE we're interested in
    for col in ppe_items:
        new_col = f"county_{col}"
        df[new_col] = df.groupby(["county", "county_fips", "date"])[col].transform("sum")
    
    return df


def categorize_ppe(row):
    n95_group = ["N-95 Respirators", "KN95 Respirators"]
    surgical_masks_group = ["Surgical Masks"]
    cloth_masks_group = ["Cloth Masks"]
    eye_protection_group = ["Goggles"]
    face_shields_group = ["Face Shields (Disposable)"]
    gloves_group = ["Examination Gloves"]
    gowns_group = ["Surgical or Examination Gowns", "Coveralls (Hospitals or EMS)"]
    
    n95 = 0
    surgical_masks = 0
    cloth_masks = 0
    eye_protection = 0
    face_shields = 0
    gloves = 0
    gowns = 0
    
    ppe = row.product_family   
    number = row.quantity_filled
    
    if any(word in ppe for word in n95_group):
        n95 = number
    if any(word in ppe for word in surgical_masks_group):
        surgical_masks = number
    if any(word in ppe for word in cloth_masks_group):
        cloth_masks = number
    if any(word in ppe for word in eye_protection_group):
        eye_protection = number        
    if any(word in ppe for word in face_shields_group):
        face_shields = number 
    if any(word in ppe for word in gloves_group):
        gloves = number 
    if any(word in ppe for word in gowns_group):
        gowns = number         
    
    return pd.Series([n95, surgical_masks, cloth_masks, 
                      eye_protection, face_shields, gloves, gowns], 
                     index=["n95", "surgical_masks", "cloth_masks", 
                            "eye_protection", "face_shields", "gloves", "gowns"])


def update_ca_ppe(**kwargs):    
    df = catalog.ca_open_data.ppe.read()
    df = clean_data(df)
    df.to_parquet(f"s3://{bucket_name}/jhu_covid19/ca-ppe.parquet")
