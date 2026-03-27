from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator  
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import os

# Path define
BASE_PATH = '/opt/airflow/data/'

def move_data_to_bronze(file_name, target_table):
    print(f"Starting ingestion for: {file_name}")
    csv_path = os.path.join(BASE_PATH, file_name)
    data = pd.read_csv(csv_path)
    
    # Cleaning columns
    data.columns = [col.upper().replace(' ', '_') for col in data.columns]
    
    hook = PostgresHook(postgres_conn_id='postgres_conn')
    conn = hook.get_sqlalchemy_engine()
    
    # if_exists='append' use karna behtar hai agar aap ek hi raw table mein data jama kar rahe hain,
    # lekin brief ke mutabiq sequential maintain karne ke liye hum table-per-month logic hi rakhte hain.
    data.to_sql(target_table, conn, schema='bronze', if_exists='append', index=False)
    print(f"Successfully moved {len(data)} rows to {target_table}")

dag_args = {
    'start_date': datetime(2024, 1, 1),
    'catchup': False
}

with DAG(
    dag_id='airbnb_master_ingestion_v2',
    default_args=dag_args,
    schedule_interval=None,
    description='Sequential ingestion with dbt trigger'
) as dag:

    # --- Step 1: Lookup Tables ---
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

    # --- Step 3: Listings (Chronological & Sequential) ---
    # Data sequence: May 2020 to April 2021 
    chronological_months = [
        '05_2020', '06_2020', '07_2020', '08_2020', '09_2020', '10_2020', 
        '11_2020', '12_2020', '01_2021', '02_2021', '03_2021', '04_2021'
    ]
    
    # Pehla sequential task start karne ke liye reference
    previous_task = load_census_2
    
    for month in chronological_months:
        ingest_task = PythonOperator(
            task_id=f'ingest_listings_{month}',
            python_callable=move_data_to_bronze,
            op_kwargs={
                'file_name': f'listings/{month}.csv', 
                'target_table': f'listings_{month}'
            }
        )
        # Sequence create karna: Har naya task purane task ke khatam hone par chalay ga 
        previous_task >> ingest_task
        previous_task = ingest_task

    # --- Step 4: dbt Trigger (End-to-End Orchestration) ---
    # Ye task dbt models ko trigger karega (Silver aur Gold layers ke liye) 
    run_dbt_transformations = BashOperator(
        task_id='trigger_dbt_models',
        bash_command='cd /opt/airflow/airbnb_project && dbt run && dbt snapshot'
    )

    # Final dependency: Akhri listing task ke baad dbt chalay ga
    previous_task >> run_dbt_transformations

    # Start dependencies
    [load_lga_codes, load_suburbs] >> load_census_1