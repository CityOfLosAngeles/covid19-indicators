"""
Test read/write with S3 bucket.

Read from URL, write to S3 as parquet, then read it from S3 again
"""

import pandas
S3_FILE_PATH = "s3://public-health-dashboard/jhu_covid19/"

COUNTY_VACCINE_URL = (
    "https://data.chhs.ca.gov/dataset/e283ee5a-cf18-4f20-a92c-ee94a2866ccd/resource/"
    "130d7ba2-b6eb-438d-a412-741bde207e1c/download/"
    "covid19vaccinesbycounty.csv"
)


df = pandas.read_csv(COUNTY_VACCINE_URL)
df.to_parquet(f"{S3_FILE_PATH}vaccination_by_county.parquet")
print("Successful write to S3")

df2 = pandas.read_parquet(f"{S3_FILE_PATH}vaccination_by_county.parquet")
print("Successful read from S3")