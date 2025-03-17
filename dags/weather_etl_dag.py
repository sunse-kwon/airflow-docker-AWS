from airflow.models import DAG
from airflow.operators.python import PythonOperator
# from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta
import json
import sys
import os



# add scrips directory to path
# sys.path.append('/opt/airflow/dags/scripts')
from scripts.extract import fetch_weather_data
from scripts.transform import process_weather_data
from scripts.load import stage_weather_data

# define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': 'sunse523@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'timezone': 'KST',
    'retry_delay': timedelta(minutes=5)
}

# define Dag 
with DAG('weather_etl_dag', default_args=default_args, start_date=datetime(2025,3,15), schedule='10 * * * *', catchup=False) as dag:
    ## fetch data from api using python operator
    fetch_task = PythonOperator(
        task_id='fetch_weather_data',
        python_callable=fetch_weather_data,
    )
    
    # Process the data and prepare it for SQL
    process_task = PythonOperator(
        task_id='process_weather_data',
        python_callable=process_weather_data,
        provide_context=True,  # Allows access to ti (TaskInstance)
    )

    stage_task = PythonOperator(
        task_id='stage_weather_data',
        python_callable=stage_weather_data,
        provide_context=True
    )



    # ## stage data using SQLExecuteQueryOperator
    # stage_task = SQLExecuteQueryOperator(
    #     task_id='stage_weather_data',
    #     conn_id='weather_connection',
    #     sql="""
    #         INSERT INTO staging_weather (raw_json, base_date, base_time, nx, ny)
    #         VALUES (%s, %s, %s, %s, %s)
    #         ON CONFLICT DO NOTHING
    #     """,
    #     # parameters= "{{ ti.xcom_pull(task_ids='process_weather_data') }}"
    # )
    # # define Task dependencies
    fetch_task >> process_task >> stage_task