import datetime as dt
import os
import pandas as pd
import numpy as np
import requests as rq

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

def print_world():
    print('Hellow world')

def data_save():
    df = pd.DataFrame(np.arange(48).reshape(8,6),columns=list('ABCDEF'))
    df.to_csv('/usr/local/airflow/dags/test_file.csv', sep=',', header=True)

def air_quality_index():
    url = 'https://api.data.gov.in/resource/3b01bcb8-0b14-4abf-b6f2-c1bfd384ba69'
    #payload passed to url parameters
    payload = {'api-key' : '579b464db66ec23bdd000001147f188762f84e124f231d0017e63d75',
                'format' : 'json',
                'offset' : 0,
                'limit' : 10000,
                #'filters[state]': 'TamilNadu'
                }
    r = rq.get(url, params=payload)
    json_data = r.json()['records']
    df_aq = pd.DataFrame(json_data)
    if not os.path.isfile('/usr/local/airflow/dags/air_quality_index.csv'):
        df_aq.to_csv('/usr/local/airflow/dags/air_quality_index.csv', sep=',', header=True)
    else :
        df_aq.to_csv('/usr/local/airflow/dags/air_quality_index.csv', sep=',', header=False, mode='a')

default_args = {
    'owner': 'Sivakguru',
    'start_date': dt.datetime(2021, 2, 9, 13, 9),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
}


with DAG('air_quality_docker',
         default_args=default_args,
         schedule_interval='@hourly',
         ) as dag:

        t1 = BashOperator(
            task_id='task_1',
            bash_command='echo "Hello World from Task 1"',
            dag=dag)

        t2 = BashOperator(
            task_id='task_2',
            bash_command='echo "Hello World from Task 2"',
            dag=dag)

        print_world = PythonOperator(task_id='print_world',
                                     python_callable=print_world,
                                     trigger_rule=TriggerRule.ALL_SUCCESS,
                                     dag=dag)

        data_save = PythonOperator(task_id='data_save',
                                    python_callable=data_save,
                                    trigger_rule=TriggerRule.ALL_SUCCESS,
                                    dag=dag)

        air_quality_index = PythonOperator(task_id='air_quality_index',
                                            python_callable=air_quality_index,
                                            trigger_rule=TriggerRule.ALL_SUCCESS,
                                            dag=dag)


        [t1,t2] >> print_world >> data_save >> air_quality_index