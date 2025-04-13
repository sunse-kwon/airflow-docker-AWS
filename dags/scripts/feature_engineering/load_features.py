import pandas as pd
from airflow.hooks.postgres_hook import PostgresHook
from sqlalchemy import create_engine
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_features(ti):
    try:
        data = ti.xcom_pull(task_ids='transform_features')
        if not data:
            logger.error(f'No data received from transform_features task')
            raise ValueError(f'No data received from transform_features task')

        # Concatenate data
        transformed_features = pd.concat(data)
        # create timestamp index for pk
        transformed_features.reset_index(inplace=True)
        if transformed_features.empty:
            logger.error(f'Transformed features dataframe is empty')
            raise ValueError(f'Transformed features dataframe is empty')

        # Define target columns
        target_columns = ['timestamp','PTY','REH','RN1','T1H','WSD','day','hour','sin_hour','cos_hour','is_weekend',
                         'day_of_week_encoded','PTY_lag1','PTY_lag2','delay_hours_lag1','delay_hours_lag2','delay_hours']
        missing_columns = [column for column in target_columns if column not in transformed_features.columns]
        if missing_columns:
            logger.error(f'Missing columns : {missing_columns}')
            raise ValueError(f'Missing columns : {missing_columns}')
        
        # Select only target columns
        transformed_features = transformed_features[target_columns]
        row_count = len(transformed_features)

        # Initialize PostgresHook to get connection details
        pg_hook = PostgresHook(postgres_conn_id='weather_connection')
        conn = pg_hook.get_conn()
        conn_details = pg_hook.get_connection('weather_connection')

        # Create SQLAlchemy engine
        db_uri = f"postgresql+psycopg2://{conn_details.login}:{conn_details.password}@{conn_details.host}:{conn_details.port}/{conn_details.schema}"
        engine = create_engine(db_uri)

        # Insert DataFrame using to_sql
        logger.info(f"Inserting {row_count} rows into features table")
        transformed_features.to_sql(
            name='feature_delays',
            con=engine,
            if_exists='append',  # Use 'replace' to overwrite table
            index=False,
            method='multi'  # Use multi-row inserts
        )
        logger.info(f"Successfully inserted {row_count} rows")
        return {'rows_inserted': row_count}
    except Exception as e:
        logger.error(f'Error in load_features: {str(e)}')
        raise