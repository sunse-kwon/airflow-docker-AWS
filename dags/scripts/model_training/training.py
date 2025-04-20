import random
import numpy as np
import pandas as pd
from sklearn.metrics import mean_squared_error
from sklearn.ensemble import RandomForestRegressor
import mlflow
import mlflow.sklearn
import logging
import os
from dotenv import load_dotenv


random.seed(42)         
np.random.seed(42)

logger = logging.getLogger(__name__)


def train_model(ti):
    load_dotenv()

    X_train = pd.DataFrame(ti.xcom_pull(task_ids='data_preparation', key='X_train'))
    X_test = pd.DataFrame(ti.xcom_pull(task_ids='data_preparation', key='X_test'))
    y_train = pd.DataFrame(ti.xcom_pull(task_ids='data_preparation', key='y_train'))
    y_test = pd.DataFrame(ti.xcom_pull(task_ids='data_preparation', key='y_test'))

    # Convert timestamp to datetime
    X_train['timestamp'] = pd.to_datetime(X_train['timestamp'])
    X_test['timestamp'] = pd.to_datetime(X_test['timestamp'])
    y_train['timestamp'] = pd.to_datetime(y_train['timestamp'])
    y_test['timestamp'] = pd.to_datetime(y_test['timestamp'])
    
    # Set timestamp as index
    X_train.set_index('timestamp',inplace=True)
    X_test.set_index('timestamp',inplace=True)
    y_train.set_index('timestamp',inplace=True)
    y_test.set_index('timestamp',inplace=True)

    # MLflow setup
    tracking_uri = os.getenv("MLFLOW_TRACKING_URI")
    if not tracking_uri:
        logger.error(f'MLFLOW_TRACKING_URI not set in .env')
        raise ValueError(f'MLFLOW_TRACKING_URI not set in .env')
    
    mlflow.set_tracking_uri(tracking_uri)  
    mlflow.set_experiment("automated_weather_delivery_delay_prediction_seoul")  

    with mlflow.start_run():
        params = {
            "n_estimators":339,
            "max_depth":17,
            "min_samples_split":9,
            "min_samples_leaf":1,
            "max_features":"sqrt",
            "random_state":42
        }
        model = RandomForestRegressor(**params)
        model.fit(X_train, y_train)

        # Evaluate (RMSE on log scale)
        y_pred_log = model.predict(X_test)
        y_pred_original = np.expm1(y_pred_log)
        y_test_original = np.expm1(y_test)
        rmse = np.sqrt(mean_squared_error(y_test_original, y_pred_original))

        # Log to MLflow
        mlflow.log_params(params)
        mlflow.log_param("data_timestamp_min", str(X_train.index.min()))
        mlflow.log_param("data_timestamp_max", str(X_train.index.max()))
        mlflow.log_metric("rmse", rmse)
        mlflow.log_dict(
            dict(zip(X_train.columns, model.feature_importances_)),
            "feature_importances.json"
        )
        mlflow.sklearn.log_model(model, "random_forest_model")
        
        # Push to XCom
        run_id = mlflow.active_run().info.run_id
        ti.xcom_push(key="run_id", value=run_id)
        ti.xcom_push(key="rmse", value=rmse)