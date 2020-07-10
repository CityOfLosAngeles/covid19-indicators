import pandas as pd

URL = (
    "https://raw.githubusercontent.com/ANRGUSC/"
    "lacounty_covid19_data/master/data/Covid-19.csv"
)

S3_FILE_PATH = "s3://public-health-dashboard/jhu_covid19/"

def clean_data():
    df = pd.read_csv(URL)
    
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
    
    # Derive columns
    df = (df.assign(
            new_cases = (df.sort_values(sort_cols).groupby(group_cols)["cases"]
                         .diff(periods=1)
                        ),
        )
    )
    
    df = (df.assign(
            cases_avg7=df.cases.rolling(window=7).mean(),
            new_cases_avg7=df.new_cases.rolling(window=7).mean(),
        )   
    )
    
    quartiles = (df.groupby("date")["cases"].describe()[["25%", "50%", "75%"]]
                 .rename(columns = {"25%": "ptile25",
                                    "50%": "ptile50",
                                    "75%" :"ptile75"})
                 .reset_index()
                )
    
    df2 = pd.merge(df, quartiles, on = "date", how = "left", validate = "m:1")
    
    integrify_me = ["ptile25", "ptile50", "ptile75"]
    for col in integrify_me:
        df2[col] = df2[col].round(0).astype(int)
    
    df2["new_cases"] = df2.new_cases.astype("Int64")

    df2.to_parquet(f"{S3_FILE_PATH}lacounty-neighborhood-time-series.parquet")
    
    return df2
   

clean_data()