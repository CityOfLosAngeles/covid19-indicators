import geopandas as gpd
import pandas as pd

URL = (
    "https://raw.githubusercontent.com/ANRGUSC/"
    "lacounty_covid19_data/master/data/Covid-19.csv"
)

POP_URL = (
    "https://raw.githubusercontent.com/ANRGUSC/"
    "lacounty_covid19_data/master/data/processed_population.csv"
)

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

S3_FILE_PATH = "s3://public-health-dashboard/jhu_covid19/"

""" DO THIS ONCE

# Function to clean data up to 7/2
def clean_data():
    df = pd.read_csv(URL)
    pop = pd.read_csv(POP_URL)
    
    pop = pop.assign(
        Population = pop.Population.astype(int)
    )
    
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
    
    # Check for duplicates
    df = df.assign(
        max_cases = df.groupby(sort_cols)["cases"].transform("max")
    ) 
    
    df = (df[df.cases == df.max_cases]
          .drop(columns = "max_cases")
          .drop_duplicates(subset = ["Region", "date", "cases"])
    )
    
    # Merge in population
    df = pd.merge(df, pop.drop(columns = ["Latitude", "Longitude"]), 
                 on = "Region", how = "left", validate = "m:1")
    
    # Derive columns
    final = derive_columns(df, sort_cols, group_cols)
    final.to_parquet(f"{S3_FILE_PATH}lacounty-neighborhood-time-series.parquet")
    
    return df3


# Update data
df = clean_data()    
"""

def derive_columns(df, sort_cols, group_cols):
    # Derive columns
    POP_DENOM = 100_000
    
    df = (df.assign(
            new_cases = (df.sort_values(sort_cols).groupby(group_cols)["cases"]
                         .diff(periods=1)
                        ),
            cases_per100k = df.cases / df.Population * POP_DENOM,
        ).sort_values(sort_cols)
        .reset_index(drop=True)
    )
    
    # Calculate rolling averages
    df = (df.assign(
            cases_avg7 = df.cases.rolling(window=7).mean(),
            new_cases_avg7 = df.new_cases.rolling(window=7).mean(),
            cases_per100k_avg7 = df.cases_per100k.rolling(window=7).mean(),
        )   
    )
   
    # Calculate quartiles
    case_quartiles = (df.groupby("date")["cases_avg7"].describe()[["25%", "50%", "75%"]]
                 .rename(columns = {"25%": "cases_p25",
                                    "50%": "cases_p50",
                                    "75%" :"cases_p75"})
                 .reset_index()
                )
    
    normalized_case_quartiles = (df.groupby("date")["cases_per100k_avg7"].describe()[["25%", "50%", "75%"]]
                 .rename(columns = {"25%": "ncases_p25",
                                    "50%": "ncases_p50",
                                    "75%" :"ncases_p75"})
                 .reset_index()
                )
    
    
    df2 = pd.merge(df, case_quartiles, on = "date", how = "left", validate = "m:1")
    df3 = pd.merge(df2, normalized_case_quartiles, on = "date", how = "left", validate = "m:1")
    
    # Add rankings
    df3["rank"] = df3.groupby("date")["cases_per100k"].rank("dense", ascending=False).astype("Int64")
    df3["max_rank"] = df3.groupby("date")["rank"].transform("max").astype(int)
    
    return df3


# Function to clean data since 7/10
def grab_data_from_layer():
    df = gpd.read_file(COUNTY_NEIGHBORHOOD_URL)

    keep_cols = ["LCITY", "COMMUNITY", "LABEL", "CONFIRMED", "DEATHS"]
    df = df[keep_cols].assign(
        date = pd.to_datetime(df.Cases_Date, unit='ms').dt.date,
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
    
    # For every Region, aggregate, in case there are unincorporated parts with no cases
    keep = ["Region", "date", "cases", "deaths"]

    df = (df.assign(
            cases = df.groupby("Region")["cases"].transform("sum"),
            deaths = df.groupby("Region")["deaths"].transform("sum"),
            date2 = pd.to_datetime(df.date),
        )[keep]
          .drop_duplicates()
    )
    
    return df


def update_neighborhood_data(**kwargs):  
    # Read in historical data
    historical_df = pd.read_parquet(f"{S3_FILE_PATH}lacounty-neighborhood-time-series.parquet")
    
    # Grab today's data
    today_df = grab_data_from_layer()
    
    # Append
    df = historical_df.append(today_df, sort=False)
    
    group_cols = ["Region"]
    sort_cols = ["Region", "date"]
    
    # Check for duplicates
    df = df.assign(
        max_cases = df.groupby(sort_cols)["cases"].transform("max")
    ) 
    
    df = (df[df.cases == df.max_cases]
          .drop(columns = "max_cases")
          .drop_duplicates(subset = ["Region", "date", "cases"])
    )
    
    # Standardize and derive columns
    def fill_in_stuff(df):
        fill_me = (df[(df.Latitude.notna()) & 
                     (df.Population.notna())][
                    ["Region", "Latitude", "Longitude", "Population"]]
                    .drop_duplicates()
                  )  

        df = pd.merge(df.drop(columns = ["Latitude", "Longitude", "Population"]), 
                      fill_me, on = "Region", how = "left", validate = "m:1")

        return df
    
    
    df = fill_in_stuff(df)
    
    cols_to_start = ["Region", "date", "date2", 
                     "Latitude", "Longitude", "Population",
                     "cases", "deaths"]
    
    final = derive_columns(df[cols_to_start], sort_cols, group_cols)
    
    # Export to S3 and overwrite old file
    final.to_parquet(f"{S3_FILE_PATH}lacounty-neighborhood-time-series.parquet")