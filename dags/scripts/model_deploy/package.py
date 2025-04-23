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
    