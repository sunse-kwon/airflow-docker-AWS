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
   logger.info(f'first data from previous task: {data.head()}')

   # target log transformation
   data['delay_hours_log'] = np.log1p(data['delay_hours'])

   # filter Seoul fulfillment center time series only. after implementation, scale out to other 3 regions (Incheon, Daegu, Icheon)
   data_seoul = data[data['city']=='Seoul'].copy()

   # Set timestamp as index
   data_seoul['timestamp'] = pd.to_datetime(data_seoul['timestamp'], format='%Y-%m-%d %H:%M:%S')  # Ensure datetime format
   data_seoul.set_index('timestamp', inplace=True)

   features=['pty','reh','rn1','t1h','wsd','day','hour','sin_hour','cos_hour','is_weekend',
       'day_of_week_encoded','pty_lag1','pty_lag2','delay_hours_lag1','delay_hours_lag2']
    
   X  = data_seoul[features]
   y = data_seoul['delay_hours_log']

   X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=False, random_state=42)

   logger.info(f'after split, check sample X_train: {X_train.head()}')
 # Reset index to include timestamp as a column
   X_train = X_train.reset_index()  
   X_test = X_test.reset_index()    
   y_train = y_train.reset_index()  
   y_test = y_test.reset_index()    

   # Convert timestamp to string for JSON serialization
   X_train['timestamp'] = X_train['timestamp'].astype(str)
   X_test['timestamp'] = X_test['timestamp'].astype(str)
   y_train['timestamp'] = y_train['timestamp'].astype(str)
   y_test['timestamp'] = y_test['timestamp'].astype(str)

   logger.info(f'last, check sample X_test: {X_test.head()}')

   # Push to XCom
   ti.xcom_push(key="X_train", value=X_train.to_dict(orient='records'))
   ti.xcom_push(key="X_test", value=X_test.to_dict(orient='records'))
   ti.xcom_push(key="y_train", value=y_train.to_dict(orient='records'))
   ti.xcom_push(key="y_test", value=y_test.to_dict(orient='records'))
    