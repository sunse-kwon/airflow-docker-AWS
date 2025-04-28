from airflow.models import DAG
import mlflow
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.sagemaker import SageMakerEndpointConfigOperator,SageMakerEndpointOperator
from datetime import datetime, timedelta

# load modules
from scripts.model_training.register import transition_to_production
from scripts.model_deploy.package import trigger_github_action



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

    trigger_github_action_task = PythonOperator(
        task_id='trigger_github_action',
        python_callable=trigger_github_action,
    )
    # Dependencies
    transition_task >> trigger_github_action_task 