#!/bin/python
#import web table
import pandas as pd
import sys
from unicodedata import normalize
from datetime import timedelta
from datetime import date

S3_FILE_PATH='s3://public-health-dashboard/jhu_covid19/'
data_source_1='http://publichealth.lacounty.gov/media/Coronavirus/locations.htm'
data_source_2=f'{S3_FILE_PATH}city-of-la-cases.csv'
#data_source_2='google_sheet_extracted_appendable.csv'
today_date = pd.to_datetime(date.today())



table1=pd.read_html(data_source_1)

i=0
found=False
while i< len(table1):
	if table1[i].keys()[0]=='CITY/COMMUNITY**':
		found=True
		break
	i=i+1

if not found:
	print("table not found")
else:
	mytab=table1[i]
	mydata=mytab.loc[mytab['CITY/COMMUNITY**']=='Los Angeles']

new_row = pd.DataFrame({'date':(pd.to_datetime('today').normalize()-timedelta(days=1)).date(), 'city_cases':mydata['Cases'].values[0], 'city_deaths':mydata['Deaths'].values[0]},index=[0])
df=pd.read_csv(data_source_2)
df = (df.drop(columns = ["city_new_cases", "city_new_deaths"])
            .assign(
                date = pd.to_datetime(df.date).dt.date,
                city_cases = df.city_cases.round(0).astype("Int64"),
                city_deaths = df.city_deaths.round(0).astype("Int64"),
            )
    )
df['date']=pd.to_datetime(df.date)
new_row['date']=pd.to_datetime(new_row.date)
#df['city_cases']=df.city_cases.astype("Int64")
#df['city_deaths']=df.city_deaths.astype("Int64")






#get max date
max_date=df['date'].max()
max_date_row=df.loc[df['date']==df['date'].max()]

#Compare current date
current_date=new_row['date'].max()
if current_date<=max_date:
    print(f'Current day data exists, {data_source_2} not updated')
#    df.to_csv(data_source_2+'.t',index=False)
    data_source_2=data_source_2+'.t'
#    sys.exit()

print('Appending current day to dataset')
df = pd.concat([df,new_row]).reset_index(drop = True)

df = df.assign(
        city_new_cases = df.sort_values("date")["city_cases"].diff(periods=1).astype("Int64"),
        city_new_deaths = df.sort_values("date")["city_deaths"].diff(periods=1).astype("Int64"),
    )


df = (df[df.date <= today_date]
            .dropna(subset = ["city_cases", "city_deaths"])
            .sort_values("date")
            .reset_index(drop=True)
    )

#print(f'writing {data_source_2}')
#df.to_csv(data_source_2,index=False)
df.to_csv('test.csv',index=False)
