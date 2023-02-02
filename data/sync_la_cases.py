"""
Pull data from DAILY COVID TABLE for
Mayor's Office Daily COVID-19 Report and upload to S3
"""
import pandas as pd
import sys
from unicodedata import normalize
from datetime import timedelta
from datetime import date


from processing_utils import default_parameters

today_date = date.today()

S3_FILE_PATH = default_parameters.S3_FILE_PATH

#Replaced google sheet source with csv from s3
#workbook = (
#    "https://docs.google.com/spreadsheets/d/"
#    "1Vk7aGL7O0ZVQRySwh6X2aKlbhYlAR_ppSyMdMPqz_aI/"
#    "export?format=xlsx&#gid=0"
#)
#sheet_name = "CASE_DATA"

data_source_1='http://publichealth.lacounty.gov/media/Coronavirus/locations.htm'
data_source_2=f'{S3_FILE_PATH}city-of-la-cases.csv'

def get_data():
    today_date = pd.to_datetime(date.today())


    table_collection=pd.read_html(data_source_1)
    i=0
    found=False
    while i< len(table1):
        if table1[i].keys()[0]=='CITY/COMMUNITY**':
            found=True
            break
        i=i+1

    if not found:
        print("CITY/COMMUNITY table not found")
        sys.exit(f"CITY/COMMUNITY table not found in {data_source_1}")

    mytab=table1[i]
    mydata=mytab.loc[mytab['CITY/COMMUNITY**']=='Los Angeles']

    new_row = pd.DataFrame(
            {
                'date':(pd.to_datetime('today')
                .normalize()-timedelta(days=1))
                .date(), 
                'city_cases':mydata['Cases'].values[0], 
                'city_deaths':mydata['Deaths'].values[0]
                },
            index=[0]
            )

    df=pd.read_csv(data_source_2)
    df = (df.drop(columns = ["city_new_cases", "city_new_deaths"])
            .assign(
                date = pd.to_datetime(df.date).dt.date,
                city_cases = df.city_cases.round(0).astype("Int64"),
                city_deaths = df.city_deaths.round(0).astype("Int64"),
                )
            )
    #match column types
    df['date']=pd.to_datetime(df.date)
    df['city_cases']=df.city_cases.astype("Int64")
    df['city_deaths']=df.city_deaths.astype("Int64")
    new_row['date']=pd.to_datetime(new_row.date)
    new_row['city_cases']=new_row.city_cases.astype("Int64")
    new_row['city_deaths']=new_row.city_deaths.astype("Int64")

    
    #get max date
    max_date=df['date'].max()
    max_date_row=df.loc[df['date']==df['date'].max()]

    #Compare current date
    current_date=new_row['date'].max()
    if current_date<=max_date:
        print(f'Current day data exists, {data_source_2} not updated')
        sys.exit()
#        df.to_csv(data_source_2+'.t',index=False)
#        data_source_2=data_source_2+'.t'

    """
    If cases are stable over 3 days, then the new_cases is reallocated and split across 3 days. 
    Valid approach used by experts, especially since confirmed cases are a function of tests conducted.
    However, we already use a 7-day rolling average to smooth out the day to day fluctuations.

    What are the implications of splitting across days, then doing a rolling average on top of that?
    Numbers will seem even more deflated?

    For now, let's create new_cases and new_deaths again, and then apply 7-day rolling average.
    Have City of LA be consistent with LA County reporting.
    """

    print('Appending current day to dataset')
    df = pd.concat([df,new_row]).reset_index(drop = True)

    df = df.assign(
            city_new_cases = df.sort_values("date")["city_cases"]
            .diff(periods=1).astype("Int64"), 
            city_new_deaths = df.sort_values("date")["city_deaths"]
            .diff(periods=1).astype("Int64"),
            )

    df = (df[df.date <= today_date]
            .dropna(subset = ["city_cases", "city_deaths"])
            .sort_values("date")
            .reset_index(drop=True)
            )

    df.to_csv(f"{S3_FILE_PATH}city-of-la-cases.csv", index=False)
    df.to_parquet(f"{S3_FILE_PATH}city-of-la-cases.parquet")


def update_la_cases_data(**kwargs):   
    get_data()
