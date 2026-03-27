from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import os

# Path define
BASE_PATH = '/opt/airflow/data/'

def move_data_to_bronze(file_name, target_table):
    print(f"Starting ingestion for: {file_name}")
    
    # Simple file reading logic
    csv_path = os.path.join(BASE_PATH, file_name)
    data = pd.read_csv(csv_path)
    
    # Cleaning columns manually taake SQL mein masla na ho
    data.columns = [col.upper().replace(' ', '_') for col in data.columns]
    
    # Connection establishing
    hook = PostgresHook(postgres_conn_id='postgres_conn')
    conn = hook.get_sqlalchemy_engine()
    
    # Loading to bronze
    data.to_sql(target_table, conn, schema='bronze', if_exists='replace', index=False)
    print(f"Successfully moved {len(data)} rows to {target_table}")

# --- DAG Definition ---
dag_args = {
    'start_date': datetime(2024, 1, 1),
    'catchup': False
}

with DAG(
    dag_id='airbnb_master_ingestion',
    default_args=dag_args,
    schedule_interval=None,
    description='Manual ingestion for Airbnb project by Shees'
) as dag:

    # --- Step 1: Lookup Tables (LGA & Suburbs) ---
    load_lga_codes = PythonOperator(
        task_id='ingest_lga_codes',
        python_callable=move_data_to_bronze,
        op_kwargs={'file_name': 'NSW_LGA/NSW_LGA_CODE.csv', 'target_table': 'nsw_lga_codes'}
    )

    load_suburbs = PythonOperator(
        task_id='ingest_suburb_mapping',
        python_callable=move_data_to_bronze,
        op_kwargs={'file_name': 'NSW_LGA/NSW_LGA_SUBURB.csv', 'target_table': 'nsw_lga_suburb'}
    )

    # --- Step 2: Census Data ---
    load_census_1 = PythonOperator(
        task_id='ingest_census_g01',
        python_callable=move_data_to_bronze,
        op_kwargs={'file_name': 'Census_LGA/2016Census_G01_NSW_LGA.csv', 'target_table': 'nsw_lga_census_2016_g01'}
    )

    load_census_2 = PythonOperator(
        task_id='ingest_census_g02',
        python_callable=move_data_to_bronze,
        op_kwargs={'file_name': 'Census_LGA/2016Census_G02_NSW_LGA.csv', 'target_table': 'nsw_lga_census_2016_g02'}
    )

    # --- Step 3: Listings (All Months) ---
    # Yahan hum months ko manually define kar rahay hain taake Graph mein saaf nazar aayein
    months = ['01_2021','02_2021','03_2021','04_2021','05_2020', '06_2020', '07_2020', '08_2020', '09_2020', '10_2020', '11_2020', '12_2020']
    
    listing_tasks = []
    for month in months:
        t = PythonOperator(
            task_id=f'ingest_listings_{month}',
            python_callable=move_data_to_bronze,
            op_kwargs={
                'file_name': f'listings/{month}.csv', 
                'target_table': f'listings_{month}'
            }
        )
        listing_tasks.append(t)

    # --- Setting Dependencies ---
    [load_lga_codes, load_suburbs] >> load_census_1 >> load_census_2 >> listing_tasks