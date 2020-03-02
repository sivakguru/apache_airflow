#pip install requests
import datetime
import requests as rq
import psycopg2 as pg
import sqlalchemy as sql
import pandas as pd
import json
import csv

#function to create table
def sql_executor(statement):
    conn = pg.connect("host = localhost dbname = postgres user = postgres password = Sivkumar_123 port = 5432")
    cur = conn.cursor()
    cur.execute(statement)
    conn.commit()
    cur.close()
    conn.close()

#variables to set schema and table name in createstatement
schema = 'prestage'
table_name = 'air_quality_index'
create_statment = """
    CREATE TABLE IF NOT EXISTS "%s"."%s"(
        id numeric(20),
        country varchar(500),
        state varchar(500),
        city varchar(500),
        station varchar(1000),
        last_update timestamp,
        pollutant_id varchar(50),
        pollutant_min numeric(20,6),
        pollutant_max numeric(20,6),
        pollutant_avg numeric(20,6),
        pollutant_unit varchar(50)
    );
    """ % (schema,table_name)

#sql_alchemy engine to run pandas postgres sql statements
engine = sql.create_engine('postgresql+psycopg2://postgres:Sivkumar_123@127.0.0.1/postgres')


#url to get data from
url = 'https://api.data.gov.in/resource/3b01bcb8-0b14-4abf-b6f2-c1bfd384ba69'
#payload passed to url parameters
payload = {'api-key' : '579b464db66ec23bdd000001147f188762f84e124f231d0017e63d75',
            'format' : 'json',
            'offset' : 0,
            'limit' : 10000,
            #'filters[state]': 'TamilNadu'
            }

#request output stored in varible (type Response)
r = rq.get(url, params=payload)

#extrat ['records'] tags from json response
data_json = r.json()['records']

#convert json response to Data frame using pandas
df=pd.DataFrame(data_json)

#filter condition to remove 'NA' values in any one of the 3 columns
df = df[~df.pollutant_min.isin(['NA']) |
        ~df.pollutant_max.isin(['NA']) |
        ~df.pollutant_avg.isin(['NA'])]

#convert the last_update column in data frame to time_stamp data type
df['last_update'] = pd.to_datetime(df['last_update'])

#to get the max_last_update from table using query
#this will be used to load only new records in to the table
max_last_update = pd.read_sql_query('select max(last_update) max_last_update from prestage.air_quality_index',
con=engine)

#if the table has no data then we assign with a dummy date
if type(max_last_update.iloc[0,0]) == type(None):
    max_last_update = pd.to_datetime(datetime.datetime.strptime("01/01/2020", "%d/%m/%Y"))
else:
    #extracting time stamp from the data frame
    max_last_update = max_last_update.iloc[0,0]

#filter the new data frame for records greater than the max of last_update in postgres data base
df_toload = df[df['last_update'] > max_last_update]

#before loading the data create the table if table doesnot exist
sql_executor(create_statment)

#load the filtered new data from data frame to the data base
df_toload.to_sql("air_quality_index",
    con = engine,
    schema = "prestage",
    if_exists='append',
    index=False)
