# airflow stuff
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime, timedelta

import requests as rq
import psycopg2 as pg
import sqlalchemy as sql
import pandas as pd
import json
import csv

#create statements for table
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

#execute create statement
def create_statement(**kwargs):
    conn = pg.connect("host = ['host'] dbname = ['dbname'] user = ['user'] password = ['password']")
    cur = conn.cursor()
    cur.execute(['statement'])
    conn.commit()
    cur.close()
    conn.close()

#download the json file
def get_data_api(**kwargs):
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
    #save Json File
    with open('/home/siva/files/air_quality_index.json', 'w') as json_file:
        json.dump(data_json, json_file)

#load json file
def load_json_file(**kwargs):
    with open('/home/siva/files/air_quality_index.json','r') as json_file:
        load_json_data = json.loads(json_file.read())
    #convert json response to Data frame using pandas
    df=pd.DataFrame(data_json)
    #filter condition to remove 'NA' values in any one of the 3 columns
    df = df[~df.pollutant_min.isin(['NA']) |
            ~df.pollutant_max.isin(['NA']) |
            ~df.pollutant_avg.isin(['NA'])]
    #convert the last_update column in data frame to time_stamp data type
    df['last_update'] = pd.to_datetime(df['last_update'])
    #sql_alchemy engine to run pandas postgres sql statements
    engine = sql.create_engine('postgresql+psycopg2://postgres:Sivkumar_123@127.0.0.1/postgres')
    #to get the max_last_update from table using query
    #this will be used to load only new records in to the table
    max_last_update = pd.read_sql_query('select max(last_update) max_last_update from prestage.air_quality_index',
    con=engine)
    #extracting time stamp from the data frame
    max_last_update = max_last_update.iloc[0,0]
    #filter the new data frame for records greater than the max of last_update in postgres data base
    df_toload = df[df['last_update'] > max_last_update]
    #load the filtered new data from data frame to the data base
    df_toload.to_sql("air_quality_index",
        con = engine,
        schema = "prestage",
        if_exists='append',
        index=False)

default_args = {
    'owner' : 'siva',
    'start_date' : datetime(2020, 2, 27),
    'retries' : 1,
    'retry_delay' : timedelta(minutes=1),
}

with DAG('air_quality_index',
        default_args = default_args,
        schedule_interval = '@hourly',
        ) as dag :

        task_1 = PythonOperator(
            task_id = 'create_table',
            provide_context = True,
            python_callable = create_statement,
            op_kwargs = {'host':'localhost',
            'dbname':'postgres',
            'user':'postgres',
            'password':'Sivkumar_123',
            'statement': create_statment},
            dag=dag
        )

        task_2 = PythonOperator(
            task_id = 'get_data_from_api',
            provide_context = True,
            python_callable = get_data_api,
            dag=dag
        )

        task_3 = PythonOperator(
            task_id = 'load_json_file',
            provide_context = True,
            python_callable = load_json_file,
            dag = dag
        )

        task_4 = BashOperator(
            task_id = 'delete_csv_file',
            bash_command = 'rm /home/siva/files/air_quality_index.json',
            dag=dag
        )

task_1 >> task_2 >> task_3 >> task_4

