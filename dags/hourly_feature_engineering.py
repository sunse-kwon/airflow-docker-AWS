from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta


# add scrips directory to path
from scripts.feature_engineering.transform_feature import transform_features
from scripts.feature_engineering.load_features import load_features



# define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email':['sunse523@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'timezone': 'KST',
    'retry_delay': timedelta(minutes=5)
}

with DAG('hourly_feature_engineering', default_args=default_args, start_date=datetime(2025,3,15), schedule_interval='10 * * * *', catchup=False) as dag:
     # Wait for upstream DAG's final task to complete
    wait_for_masters_task = ExternalTaskSensor(
        task_id='wait_for_masters',
        external_dag_id='weather_etl_star_schema_dag',
        external_task_id='mark_processed',   
        timeout=60*60,
        mode='reschedule'
    )

    # extract from master tables
    fetch_master_tables_task = SQLExecuteQueryOperator(
        task_id='fetch_master_tables',
        conn_id='weather_connection',
        sql="""
            SELECT
            fwm.measurement_value,
            dd.base_date,
            dd.year,
            dd.month,
            dd.day,
            dd.day_of_week,
            dd.is_holiday,
            dt.base_time,
            dt.hour,
            dc.category_code,
            dc.category_description,
            dc.unit,
            dl.nx,
            dl.ny,
            dl.admin_district_code,
            dl.city,
            dl.sub_address 	
            FROM fact_weather_measurement fwm 
            left join dim_date dd on fwm.date_id = dd.date_id
            left join dim_time dt on fwm.time_id = dt.time_id
            left join dim_category dc on fwm.category_id  = dc.category_id
            left join dim_location dl on fwm.location_id = dl.location_id
            WHERE 
                (dd.base_date || ' ' || dt.base_time)::TIMESTAMP >= '{{ execution_date.replace(minute=0, second=0) - macros.timedelta(hours=2) }}'
                AND (dd.base_date || ' ' || dt.base_time)::TIMESTAMP <= '{{ execution_date.replace(minute=0, second=0) }}'
        """,
        do_xcom_push=True,
    )

    # transform features
    transform_features_task = PythonOperator(
        task_id='transform_features',
        python_callable=transform_features,
    )

    # load into feature tables similar to feature store
    load_feature_table_task = PythonOperator(
        task_id='load_feature_table',
        python_callable=load_features,
    )
    # task dependencies
    wait_for_masters_task >> fetch_master_tables_task >> transform_features_task >> load_feature_table_task