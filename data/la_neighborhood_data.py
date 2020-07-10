import pandas as pd

URL = (
    "https://raw.githubusercontent.com/ANRGUSC/"
    "lacounty_covid19_data/master/data/Covid-19.csv"
)

POP_URL = (
    "https://raw.githubusercontent.com/ANRGUSC/"
    "lacounty_covid19_data/master/data/processed_population.csv"
)

S3_FILE_PATH = "s3://public-health-dashboard/jhu_covid19/"

"""
pop = pd.read_csv(POP_URL)
pop["Population"] = pop.Population.astype(int)
pop.to_parquet(f"{S3_FILE_PATH}lacounty-neighborhood-population.parquet")
"""

def clean_data():
    df = pd.read_csv(URL)
    pop = pd.read_parquet(f"{S3_FILE_PATH}lacounty-neighborhood-population.parquet")

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
    POP_DENOM = 100_000
    
    df = (df.assign(
            new_cases = (df.sort_values(sort_cols).groupby(group_cols)["cases"]
                         .diff(periods=1)
                        ),
            cases_per100k = df.cases / df.Population * POP_DENOM,
        ).sort_values(sort_cols)
        .reset_index(drop=True)
    )
    
 
    df = (df.assign(
            cases_avg7 = df.cases.rolling(window=7).mean(),
            new_cases_avg7 = df.new_cases.rolling(window=7).mean(),
            cases_per100k_avg7 = df.cases_per100k.rolling(window=7).mean(),
        )   
    )
   
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
    df3["rank"] = df3.groupby("date")["cases_per100k"].rank("dense", ascending=False)
    df3["max_rank"] = df3.groupby("date")["rank"].transform("max")
    
    df3.to_parquet(f"{S3_FILE_PATH}lacounty-neighborhood-time-series.parquet")
    
    return df3


df = clean_data()
