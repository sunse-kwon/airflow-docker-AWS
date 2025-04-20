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

    # Pull model version from previous task
    model_version = ti.xcom_pull(task_ids='push_to_model_registry', key='model_version')
    model_name = "DeliveryDelayModelSeoul"

    if not model_version:
        logger.error("No model_version from push_to_model_registry task")
        raise ValueError("No model_version provided")

    # Load MLflow tracking URI
    tracking_uri = os.getenv("MLFLOW_TRACKING_URI")
    if not tracking_uri:
        logger.error("MLFLOW_TRACKING_URI not set in .env")
        raise ValueError("MLFLOW_TRACKING_URI not set in .env")
    
    mlflow.set_tracking_uri(tracking_uri)

    try:
        # Transition to Production
        client = mlflow.tracking.MlflowClient()
        client.transition_model_version_stage(
            name=model_name,
            version=model_version,
            stage="Production",
            archive_existing_versions=True
        )
        logger.info(f"Model {model_name} version {model_version} moved to Production")

        # Log transition details
        run_id = ti.xcom_pull(task_ids='model_training', key='run_id')
        if run_id:
            with mlflow.start_run(run_id=run_id):
                mlflow.log_param("model_stage", "Production")
                mlflow.log_param("transitioned_to_production", datetime.now().isoformat())
        else:
            logger.warning("No run_id found; skipping MLflow logging")

        # Push version to XCom for deployment
        ti.xcom_push(key="production_model_version", value=model_version)

    except MlflowException as e:
        logger.error(f"MLflow error during transition to Production: {e}")
        raise ValueError(f"Failed to transition model to Production: {e}")
    
    except Exception as e:
        logger.error(f"Unexpected error during transition: {e}")
        raise ValueError(f"Transition failed: {e}")


