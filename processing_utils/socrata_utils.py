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
    
def get_socrata_columns(username, password, 
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
    return existing_table.keys()
    

def get_socrata_column_diff(username, password, 
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
    df = pd.read_csv(f"{csv_file}")
    df = df.assign(
        date = pd.to_datetime(df.date)
    )
    print("new file extra keys not in socrata")
    print(set(df.keys())-set(existing_table.keys()))
    print("socrata fields not in new file")
    print(set(existing_table.keys())-set(df.keys()))
    
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
    df=df[df.date > max_date]
    df.sort_values(by=['date'],inplace=True,ascending=True)
    df.to_csv(f"{csv_file}.date_trimmed" , index=False)
    counter=0
    for x in df.date.unique():
        counter=counter+1
        df2=df[df.date==x]
        tmpfile=f"{csv_file}.date_trimmed.day_{counter}"
        df2.to_csv(tmpfile , index=False)
        data = open(tmpfile)
    
        client.timeout = (NUM_MINUTES * 60)
        client.upsert(socrata_dataset_id, data)
        print(f"{tmpfile} updated")
        os.remove(f"{tmpfile}")   
    os.remove(f"{csv_file}.date_trimmed")   
