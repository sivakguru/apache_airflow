#load csv files to postgres data base
# airflow stuff
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime, timedelta

# for postgres access
import psycopg2 as pg

import io
import csv
import pandas as pd

def load_to_dwh(ds, **kwargs):
    conn = pg.connect("host = localhost dbname = postgres user = postgres password = Sivkumar_123")
    cur = conn.cursor()
    with open("/home/siva/files/DIM_PM_PROVINCE.csv", 'r') as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            cur.execute("INSERT INTO prestage.dim_province VALUES (%s, %s, %s, %s)", row)
    conn.commit()
    cur.close()
    conn.close()

default_args = {
    'owner' : 'siva',
    'start_date' : datetime(2020, 2, 25),
    'retries' : 1,
    'retry_delay' : timedelta(minutes=1),
}

with DAG('csv_to_postgres',
        default_args = default_args,
        schedule_interval = '@daily',
        ) as dag :
        
        task_1 = BashOperator(
            task_id = 'copy_csv_file',
            bash_command= 'cp "/mnt/c/Users/sivkumar/Documents/Project Refference/VNPT/Data/network data/DIM_PM_PROVINCE.csv" /home/siva/files/',
            dag = dag
        )
        
        task_2 = PostgresOperator(
            task_id = 'create_table',
            database = 'postgres',
            postgres_conn_id = "postgres_localhost",
            sql = """
                DROP TABLE IF EXISTS prestage.dim_province;

                CREATE TABLE prestage.dim_province(
                    province_id numeric(10),
                    code varchar(10),
                    province_name varchar(50),
                    status numeric(5)
                );
            """,
            dag = dag
        )

        task_3 = PythonOperator(
            task_id = 'insert_into_postgres',
            provide_context=True,
            python_callable = load_to_dwh,
            dag=dag
        )

        task_4 = BashOperator(
            task_id = 'delete_csv_file',
            bash_command = 'rm /home/siva/files/DIM_PM_PROVINCE.csv',
            dag=dag
        )

task_1 >> task_2 >> task_3 >> task_4