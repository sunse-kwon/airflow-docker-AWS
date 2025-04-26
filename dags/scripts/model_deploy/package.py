import mlflow
from mlflow.tracking import MlflowClient
import tarfile
import boto3
import os
import logging
from dotenv import load_dotenv

logger = logging.getLogger(__name__)


def package_and_upload_model(model_name, bucket_name, s3_key):

    load_dotenv()

     # Load MLflow tracking URI
    tracking_uri = os.getenv("MLFLOW_TRACKING_URI")
    if not tracking_uri:
        logger.error("MLFLOW_TRACKING_URI not set in .env")
        raise ValueError("MLFLOW_TRACKING_URI not set in .env")
    
    mlflow.set_tracking_uri(tracking_uri)
    client = MlflowClient()
    
    try:
        model_version = client.get_model_version_by_alias(model_name, "prod")
    except Exception as e:
        raise ValueError(f"Failed to find model version with 'prod' alias for {model_name}: {e} ")
    run_id = model_version.run_id
    artifact_uri = f'runs:/{run_id}/random_forest_model'
    model_path = mlflow.artifacts.download_artifacts(artifact_uri)

    tar_path = 'model.tar.gz'
    with tarfile.open(tar_path, "w:gz") as tar:
        tar.add(model_path, arcname="model")
    
    s3_client = boto3.client("s3")
    
    s3_key= 'models/model.tar.gz'
    s3_client.upload_file(tar_path, bucket_name, s3_key)
    os.remove(tar_path)
    


# Define callback functions
def get_model_details(**context):
    """Retrieve MLflow model version and artifact path for the 'prod' alias."""

    load_dotenv()

    # Load MLflow tracking URI
    tracking_uri = os.getenv("MLFLOW_TRACKING_URI")
    if not tracking_uri:
        logger.error("MLFLOW_TRACKING_URI not set in .env")
        raise ValueError("MLFLOW_TRACKING_URI not set in .env")
    
    mlflow.set_tracking_uri(tracking_uri)
    client = MlflowClient()

    model_version = client.get_model_version_by_alias('DeliveryDelayModelSeoul', 'prod')
    
    context['ti'].xcom_push(key='model_version', value=model_version.version)
    context['ti'].xcom_push(key='run_id', value=model_version.run_id)
    context['ti'].xcom_push(key='artifact_uri', value=model_version.source)


def push_mlflow_model(**context):
    """Push MLflow model to SageMaker using push_model_to_sagemaker."""
    artifact_uri = context['ti'].xcom_pull(task_ids='get_model_details', key='artifact_uri')
    model_version = context['ti'].xcom_pull(task_ids='get_model_details', key='model_version')
    
    # Configure parameters
    model_name = 'DeliveryDelayModelSeoul'
    role_arn = 'arn:aws:iam::785685275217:role/service-role/SageMaker-mlops'
    bucket_name = 'package-model-for-sagemaker-deploy'
    image_uri = '785685275217.dkr.ecr.eu-central-1.amazonaws.com/mlflow:2.21.3'
    region = 'eu-central-1'
    
    try:
        mlflow.sagemaker.push_model_to_sagemaker(
            model_name=model_name,
            model_uri=artifact_uri,  # Or use 'models:/MyModel/Prod'
            execution_role_arn=role_arn,
            bucket=bucket_name,
            image_url=image_uri,
            region_name=region,
            flavor='sklearn',  # Adjust based on your model (e.g., 'python_function', 'xgboost')
        )
        print(f"Successfully pushed model version {model_version} to SageMaker as {model_name}")
    except Exception as e:
        print(f"Error pushing model: {str(e)}")
        raise

