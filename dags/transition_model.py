from airflow.models import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


from scripts.model_training.register import transition_to_production



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


with DAG('transition_model_to_production', default_args=default_args, start_date=datetime(2025,4,10), schedule_interval='@once', catchup=False) as dag:
    transition_task = PythonOperator(
        task_id="transition_to_production",
        python_callable=transition_to_production
    )