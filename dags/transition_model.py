from airflow.models import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


from scripts.model_training.register import transition_to_production
from scripts.model_deploy.package import package_and_upload_model


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

    package_and_upload_model_task = PythonOperator(
        task_id="package_and_upload_model",
        python_callable=package_and_upload_model,
        op_kwargs={
            "model_name":"DeliveryDelayModelSeoul",
            "bucket_name":"package-model-for-sagemaker-deploy",
            "s3_key":"models/model.tar.gz"
        }
    )
    # Dependencies
    transition_task >> package_and_upload_model_task