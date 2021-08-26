"""
Functions to push datasets to Socrata open data portal.
"""
import os
import pandas as pd
import sodapy

def overwrite_socrata_table(username, password, 
                            csv_file, socrata_dataset_id, NUM_MINUTES=20):  
    """
    username: str, Socrata username
    password: str, Socrata password
    csv_file: str, path to local CSV file used to overwrite Socrata table
    socrata_dataset_id: str, unique dataset ID for Socrata table
    NUM_MINUTES: int, number of minutes for the timeout 
    """
    client = sodapy.Socrata("data.lacity.org", 
                        app_token = None,
                        username = username,
                        password = password)
    
    data = open(f"{csv_file}")
    
    client.timeout = (NUM_MINUTES * 60)
    client.replace(socrata_dataset_id, data)
    
    print(f"{csv_file} updated")
    os.remove(f"{csv_file}")
    
    
def upsert_socrata_rows(username, password, 
                        csv_file, socrata_dataset_id, NUM_MINUTES=5):
    """
    username: str, Socrata username
    password: str, Socrata password
    csv_file: str, path to local CSV file used to overwrite Socrata table
    socrata_dataset_id: str, unique dataset ID for Socrata table
    NUM_MINUTES: int, number of minutes for the timeout 
    """
    client = sodapy.Socrata("data.lacity.org", 
                    app_token = None,
                    username = username,
                    password = password)
    
    # Grab existing table in Socrata and find where it leaves off
    existing_table = client.get(socrata_dataset_id)
    existing_table = pd.DataFrame(existing_table)
    max_date = pd.to_datetime(existing_table.date.max())
    
    df = pd.read_csv(f"{csv_file}")
    df = df.assign(
        date = pd.to_datetime(df.date)
    )
    
    # Now, overwrite the local CSV with just the rows we need to upsert
    df[df.date > max_date].to_csv(f"{csv_file}" , index=False)
    
    data = open(f"{csv_file}")
    
    client.timeout = (NUM_MINUTES * 60)
    client.upsert(socrata_dataset_id, data)

    print(f"{csv_file} updated")
    os.remove(f"{csv_file}")   