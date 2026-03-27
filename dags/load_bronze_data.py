from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import os

# Files ka path (Docker ke andar /opt/airflow/data hota hai)
DATA_PATH = '/opt/airflow/data/'

def load_csv_to_postgres(file_name, table_name):
    # 1. File read karein
    df = pd.read_csv(os.path.join(DATA_PATH, file_name))
    
    # 2. Postgres se connect karein
    postgres_hook = PostgresHook(postgres_conn_id='postgres_conn')
    engine = postgres_hook.get_sqlalchemy_engine()
    
    # 3. Bronze schema mein data load karein
    df.to_sql(table_name, engine, schema='bronze', if_exists='replace', index=False)
    print(f"{table_name} loaded successfully!")

with DAG(
    dag_id='ingest_airbnb_bronze',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

# Task 1: NSW_LGA folder ke andar hai
    load_lga_codes = PythonOperator(
        task_id='load_lga_codes',
        python_callable=load_csv_to_postgres,
        op_kwargs={'file_name': 'NSW_LGA/NSW_LGA_CODE.csv', 'table_name': 'nsw_lga_codes'}
    )

    # Task 2: NSW_LGA folder ke andar hai
    load_suburb_mapping = PythonOperator(
        task_id='load_suburb_mapping',
        python_callable=load_csv_to_postgres,
        op_kwargs={'file_name': 'NSW_LGA/NSW_LGA_SUBURB.csv', 'table_name': 'nsw_lga_suburb'}
    )

    # Task 3: Census_LGA folder ke andar hai
    load_census_g01 = PythonOperator(
        task_id='load_census_g01',
        python_callable=load_csv_to_postgres,
        op_kwargs={'file_name': 'Census_LGA/2016Census_G01_NSW_LGA.csv', 'table_name': 'nsw_lga_census_2016_g01'}
    )
    
    # Task 4: Census_LGA folder ke andar hai
    load_census_g02 = PythonOperator(
        task_id='load_census_g02',
        python_callable=load_csv_to_postgres,
        op_kwargs={'file_name': 'Census_LGA/2016Census_G02_NSW_LGA.csv', 'table_name': 'nsw_lga_census_2016_g02'}
    )

    # Task 5: listings folder ke andar hai
    load_listings = PythonOperator(
        task_id='load_listings',
        python_callable=load_csv_to_postgres,
        op_kwargs={'file_name': 'listings/05_2020.csv', 'table_name': 'listings_05_2020'}
    )
    
    
    load_lga_codes >> load_suburb_mapping >> load_census_g01 >> load_census_g02 >> load_listings