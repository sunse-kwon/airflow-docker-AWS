import mlflow
import logging
logger = logging.getLogger(__name__)


def validate_model(ti):
    rmse = ti.xcom_pull(task_ids='model_training', key='rmse')
    threshold = 0.5
    if rmse > threshold:
        logger.info(f'model RMSE {rmse} exeeds threshold {threshold}')
        raise ValueError(f'model RMSE {rmse} exeeds threshold {threshold}')
    run_id = ti.xcom_pull(task_ids='model_training', key='run_id')
    with mlflow.start_run(run_id=run_id):
        mlflow.log_metric("validation_passed", 1)