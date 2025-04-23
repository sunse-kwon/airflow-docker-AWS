from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.sagemaker import SageMakerModelOperator, SageMakerEndpointConfigOperator,SageMakerEndpointOperator
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

    # package_and_upload_model_task = PythonOperator(
    #     task_id="package_and_upload_model",
    #     python_callable=package_and_upload_model,
    #     op_kwargs={
    #         "model_name":"DeliveryDelayModelSeoul",
    #         "bucket_name":"package-model-for-sagemaker-deploy",
    #         "s3_key":"models/model.tar.gz"
    #     }
    # )

    push_model_task = BashOperator(
    task_id="push_model",
    bash_command=(
        "mlflow sagemaker push-model "
        "-n DeliveryDelayModelSeoul "
        "-m s3://artifact-store-sun/mlruns/3/d639258af272416184e0e574e3189d7b/artifacts/random_forest_model "
        "-e arn:aws:iam::785685275217:role/service-role/SageMaker-mlops "
        "-b package-model-for-sagemaker-deploy "
        "-i 785685275217.dkr.ecr.eu-central-1.amazonaws.com/mlflow:2.21.3 "
        "--region-name eu-central-1"
        )
    )
    
    # sagemaker_model_task = SageMakerModelOperator(
    #     task_id="create_model",
    #     aws_conn_id='aws_default',
    #     config={
    #         "ModelName": "mlflow-model",
    #         "PrimaryContainer": {
    #             "Image":"763104351884.dkr.ecr.eu-central-1.amazonaws.com/mlflow-pyfunc:2.13.2",
    #             "ModelDataUrl": "s3://package-model-for-sagemaker-deploy/models/model.tar.gz"
    #         },
    #         "ExecutionRoleArn": "arn:aws:iam::785685275217:role/service-role/SageMaker-mlops"
    #     }
    # )

    # Dependencies
    transition_task >>  push_model_task 