import mlflow
import logging
import os
from dotenv import load_dotenv
from mlflow.exceptions import MlflowException
from datetime import datetime

logger = logging.getLogger(__name__)


def push_to_model_registry(ti):
    load_dotenv()

    # Pull model URI
    model_uri = ti.xcom_pull(task_ids='export_model', key='model_uri')
    run_id = ti.xcom_pull(task_ids='model_training', key='run_id')

    if not model_uri or not run_id:
        logger.error(f'No model_uri from export_model task or run_id from export_model task')
        raise ValueError(f'No model_uri or run_id provided')
    
    # Load MLflow tracking URI
    tracking_uri = os.getenv("MLFLOW_TRACKING_URI")
    if not tracking_uri:
        logger.error(f'MLFLOW_TRACKING_URI not set in .env')
        raise ValueError(f'MLFLOW_TRACKING_URI not set in .env')
    
    mlflow.set_tracking_uri(tracking_uri)

    try:
         # Register model
        model_name = 'DeliveryDelayModelSeoul'
        model_version = mlflow.register_model(model_uri, model_name)
        logger.info(f"Registered model {model_name} version {model_version.version}")

        # Transition to Staging
        client = mlflow.tracking.MlflowClient()
        client.transition_model_version_stage(
            name=model_name,
            version=model_version.version,
            stage="Staging"
        )
        logger.info(f"Model version {model_version.version} moved to Staging")

        # Log registration details
        with mlflow.start_run(run_id=run_id):
            mlflow.log_param('registered_model_name', model_name)
            mlflow.log_param('model_version', model_version)
            mlflow.log_param('model_stage', 'Staging')

        # Push version to Xcom
        ti.xcom_push(key="model_version", value=model_version.version)

    except MlflowException as e:
        logger.error(f"MLflow error during registration: {e}")
        raise ValueError(f"Failed to register model: {e}")
    
    except Exception as e:
        logger.error(f'Unexpected error during registration: {e}')
        raise ValueError(f'Registration failed: {e}')

   

def transition_to_production(ti):

    load_dotenv()

    model_name = "DeliveryDelayModelSeoul"


    # Load MLflow tracking URI
    tracking_uri = os.getenv("MLFLOW_TRACKING_URI")
    if not tracking_uri:
        logger.error("MLFLOW_TRACKING_URI not set in .env")
        raise ValueError("MLFLOW_TRACKING_URI not set in .env")
    
    mlflow.set_tracking_uri(tracking_uri)
    client = mlflow.tracking.MlflowClient()

    try:
        # Transition to Production
        versions = client.get_latest_versions(model_name, stages=["Staging"])
        if not versions:
            logger.error("No versions found in Staging for model %s", model_name)
            raise ValueError("No Staging versions available")
        
        model_version = versions[0].version
        logger.info("Retrieved latest Staging version: %s for model %s", model_version, model_name)
    
        client.transition_model_version_stage(
            name=model_name,
            version=model_version,
            stage="Production",
            archive_existing_versions=True
        )
        logger.info(f"Model {model_name} version {model_version} moved to Production")

        # Log transition details to model version (no run_id needed)
        client.update_model_version(
            name=model_name,
            version=model_version,
            description=f"Transitioned to Production on {datetime.now().isoformat()}"
        )
        logger.info(f"Updated model version description with transition details")


        # Push version to XCom for deployment
        ti.xcom_push(key="production_model_version", value=model_version)

    except MlflowException as e:
        logger.error(f"MLflow error during transition to Production: {e}")
        raise ValueError(f"Failed to transition model to Production: {e}")
    
    except Exception as e:
        logger.error(f"Unexpected error during transition: {e}")
        raise ValueError(f"Transition failed: {e}")


