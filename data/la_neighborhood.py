import geopandas as gpd
import pandas as pd
import pytz

from processing_utils import default_parameters

from datetime import datetime

COMMIT = "8feed42789d9814152c478a57c6d370167f8df90"

HISTORICAL_URL = (
    "https://raw.githubusercontent.com/ANRGUSC/"
    f"lacounty_covid19_data/{COMMIT}/data/Covid-19.csv"
)


S3_FILE_PATH = default_parameters.S3_FILE_PATH

"""
Between 7/14/20 - 1/21/21, used a function to clean ESRI feature layer and 
append it to our parquet.


After 1/21, the values stopped getting updated, even though the layer was overwritten
When compared to the RShiny table by communities, the values were much lower.

Adjust the function to clean RShiny csv table instead of ESRI feature layer.
"""

# https://services.arcgis.com/RmCCgQtiZLDCtblq/arcgis/rest/services/COVID19_Current_Cases_For_Display/FeatureServer/0
COUNTY_NEIGHBORHOOD_URL = (
    "https://services.arcgis.com/RmCCgQtiZLDCtblq/ArcGIS/rest/services/COVID19_Current_Cases_For_Display/"
    "FeatureServer/0/query?where=1%3D1&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&"
    "spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter"
    "&returnGeodetic=false&outFields=OBJECTID%2C+LCITY%2C+COMMUNITY%2C+LABEL%2C+CONFIRMED%2C+"
    "SUSPECTED%2C+RECOVERED%2C+DEATHS%2C+Cases_Date%2C+POPULATION%2C+DEATH_RATE%2C++RATE&"
    "returnGeometry=true&returnCentroid=false&featureEncoding=esriDefault&multipatchOption=xyFootprint&"
    "maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&"
    "returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&"
    "returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&"
    "groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&"
    "returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pgeojson&token="
)


""" DO THIS ONCE

# Function to clean data up to 7/13
def clean_data():
    df = pd.read_csv(HISTORICAL_URL)
    
    df = (df.assign(
            date = pd.to_datetime(df["Time Stamp"]).dt.date,
            date2 = pd.to_datetime(df["Time Stamp"]),
            cases = df["Number of cases"].fillna(0).astype(int),
        ).drop(columns = ["Time Stamp", "Number of cases"])
          .sort_values(["date", "Region"])
          .reset_index(drop=True)
    )
    
    group_cols = ["Region"]
    sort_cols = ["Region", "date"]

    '''
    Check for duplicates

    The duplicates should be aggregated.
    Looks to be a product of the parsing, where the parsing took away "Unincorporated - " and "City of "
    For some cities, such as Bradbury, Whittier, Arcadia, etc, the unincorporated portions can lie in another 
    aggregate_region. Our crosswalk addresses this. Technically, the unincorporated ones probaby have
    much lower case counts, and we can pick where to send them. Do we want to though?

    Instead, let's take care of it in the crosswalk, and group all the duplicate Region names into the same
    aggregate_region. So, unincorporated parts that belong in another aggregate_region instead belong to 
    the where the Region name is the same. Ex: Unincorporated - Arcadia belongs with City of Arcadia, and inherits
    the same aggregate_region as City of Arcadia. The populations then aren't exact, because we're shifting things
    around, but this is a much more straightforward aggregation than picking a max / min value
    and assigning the min value to be unincorporated, since we don't know that is true for sure.
    '''

    keep_cols = ["Region", "Longitude", "Latitude", "date", "date2"]
    df = df.groupby(keep_cols).agg({"cases": "sum"}).reset_index()
    
    # Write to S3
    df.to_parquet(f"{S3_FILE_PATH}la-county-neighborhood-time-series.parquet")
    
    return df


# Function to clean data since 7/14
def grab_data_from_layer():
    df = gpd.read_file(COUNTY_NEIGHBORHOOD_URL)

    keep_cols = ["LCITY", "COMMUNITY", "LABEL", "CONFIRMED", "DEATHS"]
    df = df[keep_cols].assign(
        date = pd.to_datetime(df.Cases_Date, unit='ms').dt.date,
        date2 = pd.to_datetime(df.Cases_Date, unit='ms').dt.normalize(),
        cases = df.CONFIRMED.fillna(0).astype(int),
        deaths = df.DEATHS.fillna(0).astype(int),
    )

    def clean_up_city(row):
        if (row.LCITY != "Los Angeles") and (row.LCITY != "Unincorporated"):
            return row.LCITY
        if (row.LCITY == "Los Angeles") or (row.LCITY == "Unincorporated"):
            return row.COMMUNITY
        if row.LABEL == "County of Los Angeles":
            return ""

    df["Region"] = df.apply(clean_up_city, axis=1)  
    
    # For every Region, aggregate, so that these match how the historical data was parsed
    df = (df.assign(
            cases = df.groupby("Region")["cases"].transform("sum"),
            deaths = df.groupby("Region")["deaths"].transform("sum"),
        ).drop(columns = ["CONFIRMED", "DEATHS"])
          .drop_duplicates()
    )
    
    return df


# Update data
df = clean_data()   
today_df = grab_data_from_layer()
"""

RSHINY_CASES = f"{S3_FILE_PATH}la-county-neighborhood-rshiny.csv"

# Function to clean data since 2/1/21
def grab_today_from_rshiny():
    df = pd.read_csv(RSHINY_CASES)
    
    df = (df[["neighborhood", "cases_final", "deaths_final", "date"]]
          .assign(
              date2 = pd.to_datetime(df.date)
          ).rename(columns = {
              "neighborhood": "Region", 
              "cases_final": "cases", 
              "deaths_final": "deaths",
          })
         )
    
    # To match crosswalk, we will have to adjust `neighborhood` column so we can append with historical
    # Pattern is City of, Los Angeles - , and Unincorporated - 
    # In our crosswalk, we also have Los Angeles - , Unincorporated - , but need to fix the incorporated cities
    df["Region"] = (df.Region.str.replace("City of ", "")
                        .str.replace("Unincorporated - ", "")
                        .str.replace("Los Angeles - ", "")
                        .str.strip()
                       )
    
    # Make sure there are no duplicates by date, if there are, only keep max
    # Shouldn't be, unless we run script 2x in a day
    group_cols = ["Region", "date2"]
    df = (df.assign(
            cases = df.groupby(group_cols)["cases"].transform("max"),
            deaths = df.groupby(group_cols)["deaths"].transform("max"),
        ).drop_duplicates(subset = ["Region", "date2", "cases", "deaths"])
    ).sort_values(group_cols).reset_index(drop=True)
    
    return df


def update_neighborhood_data():
    historical_df = pd.read_parquet(f"{S3_FILE_PATH}la-county-neighborhood-time-series.parquet")
    today_df = grab_today_from_rshiny()    
    
    replacement_names = {
        "Park LaBrea": "Park La Brea", 
        "Pico": "Pico-Union",
        "San Francisquito Canyon/Bouquet C": "San Francisquito Canyon/Bouquet Canyon",
        "Temple": "Temple-Beaudry",
    }

    historical_df["Region"] = historical_df.apply(lambda x: replacement_names[x.Region] 
                                                  if x.Region in replacement_names 
                                                  else x.Region, axis=1)
    
    # Append together
    combined = historical_df.append(today_df, sort=False)

    # We'll fill in all the missing info, Lat/Lon, LCITY, COMMUNITY, LABEL
    fill_me_cols = ["Longitude", "Latitude", "LCITY", "COMMUNITY", "LABEL"]
    keep_me = ["Region"] + fill_me_cols
    
    fill_coords = (combined[combined.Longitude.notna()]
                   .sort_values(["Region", "date2"], ascending = [True, False])
                   [keep_me]
                   .drop_duplicates(subset = "Region", keep = "first")
                   .reset_index(drop=True)
              )

    combined2 = pd.merge(combined.drop(columns = fill_me_cols), 
                         fill_coords, 
                         on = "Region", 
                         how = "left", 
                         validate = "m:1")
   
    # Drop duplicates - 1st pass
    combined2 = combined2.drop_duplicates(subset=["Region", "date", "date2", 
                                                  "cases", "deaths", 
                                                  "LCITY", "COMMUNITY", "LABEL"])

    # Drop duplicates - 2nd pass
    group_cols = ["Region", "date", "date2"]

    # Check the neighborhoods particularly problematic to make sure this keeps the obs we want
    # Check El Monte, Arcadia, Whittier
    combined2["obs"] = combined2.groupby(group_cols).cumcount() + 1
    combined2["max_obs"] = combined2.groupby(group_cols)["obs"].transform("max")

    final = (combined2[(combined2.max_obs==1) | 
                 ((combined2.LABEL.notna()) & (combined2.max_obs > 1))]
                 .drop(columns = ["obs", "max_obs"])
                )
    
    # Fix dtypes    
    final = final.astype({
        "cases": "Int64", 
        "deaths": "Int64", 
        "date": "str",
    })

    final = (final.sort_values(["Region", "date", "date2", "LABEL"])
             .reset_index(drop=True)
            )
    
    final.to_parquet(f"{S3_FILE_PATH}la-county-neighborhood-time-series.parquet")
    final.to_csv(f"{S3_FILE_PATH}la-county-neighborhood-time-series.csv", index=False)
