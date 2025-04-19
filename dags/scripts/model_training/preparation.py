import random
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
import logging

random.seed(42)         
np.random.seed(42)

logger = logging.getLogger(__name__)


def prepare_data(ti):
    query_result = ti.xcom_pull(task_ids='data_extraction', key='query_results')
    if not query_result:
        logger.error(f'No data from data_extraction task')
        raise ValueError(f'Empty query result')
    
    # Example: Convert to DataFrame
    data = pd.DataFrame(query_result['data'], columns=query_result['columns'])
    
    # target log transformation
    data['delay_hours_log'] = np.log1p(data['delay_hours'])

    # filter Seoul fulfillment center time series only. after implementation, scale out to other 3 regions (Incheon, Daegu, Icheon)
    data_seoul = data[data['city']=='Seoul'].copy()

    # Set timestamp as index
    data_seoul['timestamp'] = pd.to_datetime(data_seoul['timestamp'], format='%Y-%m-%d %H:%M:%S.%f')  # Ensure datetime format
    data_seoul.set_index('timestamp', inplace=True)

    features=['PTY','REH','RN1','T1H','WSD','day','hour','sin_hour','cos_hour','is_weekend',
       'day_of_week_encoded','PTY_lag1','PTY_lag2','delay_hours_lag1','delay_hours_lag2']
    
    X  = data_seoul[features]
    y = data_seoul['delay_hours_log']

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=False, random_state=42)

    # Push to XCom
    ti.xcom_push(key="X_train", value=X_train.to_dict())
    ti.xcom_push(key="X_test", value=X_test.to_dict())
    ti.xcom_push(key="y_train", value=y_train.to_dict())
    ti.xcom_push(key="y_test", value=y_test.to_dict())
    