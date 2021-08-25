import pandas as pd

from processing_utils import socrata_utils

import credentials

SOCRATA_USER = credentials.SOCRATA_USERNAME
SOCRATA_PASSWORD = credentials.SOCRATA_PASSWORD


DATAFRAME_DICT = {
    # key: socrata_dataset_id
    # value: csv file
    "jsff-uc6b": "us-county-time-series.csv",
    "fvye-93wd": "la-county-neighborhood-time-series.csv",
    "rpp7-mevy": "vaccinations-by-county.csv",
    "iv7a-6rrq": "vaccinations-by-demographics-county.csv"
}

for socrata_id, filename in DATAFRAME_DICT.items():
    # So far, all the datasets need some extra processing to make sure Socrata table schema is correct
    if (("us-county" in filename) or 
        ("neighborhood" in filename) or 
        ("vaccinations" in filename)):
        df = socrata_utils.extra_processing(filename)
    else:
        df = pd.read_csv(f"{S3_FILE_PATH}{filename}")
    
    # Write the full table out as CSV 
    df.to_csv(f"{filename}", index=False)
    
    #socrata_utils.overwrite_socrata_table(SOCRATA_USER, SOCRATA_PASSWORD, filename, socrata_dataset_id = socrata_id)
    socrata_utils.upsert_socrata_rows(SOCRATA_USER, SOCRATA_PASSWORD, 
                                       filename, socrata_dataset_id = socrata_id)
