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
        
        client = mlflow.tracking.MlflowClient()

        # Remove existing 'staging' alias
        existing_versions = client.search_model_versions(f"name='{model_name}'")
        for existing_version in existing_versions:
            if "staging" in existing_version.aliases:
                client.delete_registered_model_alias(name=model_name, alias="staging")
                logger.info(f"Removed existing 'staging' alias from version {existing_version.version}")
        
        # Set sgaging alias
        client.set_registered_model_alias(
            name=model_name,
            alias='staging',
            version=model_version.version
        )
        logger.info(f"Model version {model_version.version} assgined 'staging' alias " )

        client.set_model_version_tag(
            name=model_name,
            version=model_version.version,
            key='stage',
            value='staging'
        )
        logger.info(f"Model version {model_version.version} tagged with stage=staging")

        # Log registration details
        with mlflow.start_run(run_id=run_id):
            mlflow.log_param('registered_model_name', model_name)
            mlflow.log_param('model_version', model_version)
            mlflow.log_param('model_alias', 'staging')

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
        # old version of prod alias
        old_prod_version = client.get_model_version_by_alias(name=model_name, alias="prod")
        logger.info(f"Found existing 'prod' version: {old_prod_version.version}")
        # Assign 'archived' alias to old 'prod' version
        client.set_registered_model_alias(
            name=model_name,
            alias="archived",
            version=old_prod_version.version
        )
        logger.info(f"Assigned 'archived' alias to old 'prod' version {old_prod_version.version}")
        
        # Update tag to indicate it's no longer active
        client.set_model_version_tag(
            name=model_name,
            version=old_prod_version.version,
            key="stage",
            value="archived"
        )
        logger.info(f"Updated old 'prod' version {old_prod_version.version} tag to stage=archived")

        # Transition to Production
        staging_version = client.get_model_version_by_alias(name=model_name,alias='staging')
        logger.info(f"Retrieved staging version: {staging_version.version} for model {model_name}")

        if not staging_version:
            logger.error("No versions found with 'staging' alias for model %s", model_name)
            raise ValueError("No staging versions available")
        
        model_version = staging_version.version
        logger.info("Retrieved latest Staging version: %s for model %s", model_version, model_name)
    
        client.set_registered_model_alias(
            name=model_name,
            alias="prod",
            version=model_version
        )
        logger.info(f"Model {model_name} version {model_version} assigned 'prod' alias")

        client.set_model_version_tag(
            name=model_name,
            version=model_version,
            key='stage',
            value='production'
        )
        logger.info(f"Model version {model_version} tagged with stage=production")

        client.delete_registered_model_alias(
            name=model_name,
            alias='staging'
        )
        logger.info(f"Removed 'staging' alias from model {model_name} version {model_version}")
        
        # Log transition details to model version (no run_id needed)
        client.update_model_version(
            name=model_name,
            version=model_version,
            description=f"Transitioned to Production with 'prod' alias on {datetime.now().isoformat()}"
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


