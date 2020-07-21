"""
Functions to clean up neighborhood data
and feed into interactive charts
"""
import pandas as pd

S3_FILE_PATH = "s3://public-health-dashboard/jhu_covid19/"

NEIGHBORHOOD_URL = f"{S3_FILE_PATH}lacounty-neighborhood-time-series.parquet"

CROSSWALK_URL = f"{S3_FILE_PATH}la_neighborhoods_population_crosswalk.parquet"

def clean_data():
    df = pd.read_parquet(NEIGHBORHOOD_URL)
    crosswalk = pd.read_parquet(CROSSWALK_URL)
    
    # Get rid of duplicates
    # We keep the incorporated and unincorporated labels because 
    # we need to identify duplicates when we append each time DAG runs
    df = (df[df.Region != "Long Beach"]
          .drop_duplicates(subset = ["Region", "date", "date2", "cases", "deaths"])
          .drop(columns = ["LCITY", "COMMUNITY", "LABEL"])
         )
    
    # Our crosswalk is more extensive, get rid of stuff so we can have a m:1 merge
    crosswalk = (crosswalk[crosswalk.Region.notna()]
                 [["Region", "aggregate_region", "population"]]
                 .drop_duplicates()
            )
    
    # Merge in pop
    df = pd.merge(df, crosswalk, on = "Region", how = "inner", validate = "m:1")
    
    # Aggregate 
    keep_cols = ["aggregate_region", "population", "date", "date2"]
    aggregated = (df.groupby(keep_cols)
                  .agg({"cases": "sum", "deaths": "sum"})
                  .reset_index()
                 )    
    
    sort_cols = ["aggregate_region", "date", "date2"]
    group_cols = ["aggregate_region"]
    final = derive_columns(aggregated, sort_cols, group_cols)
    
    return final


def derive_columns(df, sort_cols, group_cols):
    # Derive columns
    POP_DENOM = 100_000
    
    df = (df.assign(
            new_cases = (df.sort_values(sort_cols).groupby(group_cols)["cases"]
                         .diff(periods=1)
                        ),
            cases_per100k = df.cases / df.population * POP_DENOM,
        ).sort_values(sort_cols)
        .reset_index(drop=True)
    )
    
    df = df.assign(
        new_cases = df.new_cases.fillna(0)
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
    