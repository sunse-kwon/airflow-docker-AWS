
import mlflow
import logging
from mlflow.exceptions import MlflowException
from sklearn.ensemble import RandomForestRegressor
from datetime import datetime
import os
from dotenv import load_dotenv


logger = logging.getLogger(__name__)


def export_model(ti):
    load_dotenv()

    # Pull run_id
    run_id = ti.xcom_pull(task_ids='model_training', key='run_id')
    if not run_id:
        logger.error(f'No run_id from model_training task')
        raise ValueError(f'No run_id provided')
    
    # Load MLflow tracking URI
    tracking_uri = os.getenv("MLFLOW_TRACKING_URI")
    if not tracking_uri:
        logger.error(f'MLFLOW_TRACKING_URI not set in .env')
        raise ValueError(f'MLFLOW_TRACKING_URI not set in .env')
    
    mlflow.set_tracking_uri(tracking_uri)

    # Generate and validate model URI for registeration
    model_uri = f'runs:/{run_id}/random_forest_model'
    try:
        client = mlflow.tracking.MlflowClient()

        # Validate aftifact exist
        artifact_info = client.list_artifacts(run_id, path='random_forest_model')
        for artifact in artifact_info:
            if not any(artifact.path == "random_forest_model/MLmodel"):
                logger.error(f'Model artifact not found for run_id {run_id}')
                raise ValueError(f'Model artifact not found for run_id {run_id}')
        
        # Verify model format
        model = mlflow.sklearn.load_model(model_uri)
        if not isinstance(model, RandomForestRegressor):
            logger.error(f'Model is not RandomForestRegressor for run_id {run_id}')
            raise ValueError(f'Model is not RandomForestRegressor for run_id {run_id}')
        
        # Log export metadata
        with mlflow.start_run(run_id=run_id):
            mlflow.log_param("export_timestamp", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            mlflow.log_param('model_uri', model_uri)
            mlflow.log_param('artifact_status', 'verified')
            logger.info(f'Prepared model URI: {model_uri} for run_id {run_id}')

        # Xcom push model uri
        ti.xcom_push(key="model_uri", value=model_uri)

    except MlflowException as e:
        logger.error(f'MLflow error during export: {e}')
        raise ValueError(f'Failed to export model: {e}')
    except Exception as e:
        logger.error(f'Unexpected error during export: {e}')
        raise ValueError(f'Export failed: {e}')
        
    