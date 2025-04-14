import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
import psycopg2
from psycopg2.extras import execute_values
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
        cursor = conn.cursor()

        records = [tuple(row) for row in transformed_features.itertuples(index=False)]
        logger.info(f'debugging 1st step, check records :{records}')
        insert_query = f"""
        INSERT INTO feature_delays (
            timestamp, PTY, REH, RN1, T1H, WSD, day, hour, sin_hour, cos_hour, is_weekend,
            day_of_week_encoded, PTY_lag1, PTY_lag2, delay_hours_lag1, delay_hours_lag2, delay_hours
        ) VALUES %s
        
        """
        # ON CONFLICT (timestamp) DO NOTHING
        # Insert data
        logger.info(f"Inserting {row_count} rows into features table")
        execute_values(cursor, insert_query, records)
        conn.commit()

        # Verify inserted rows
        cursor.execute(f"SELECT COUNT(*) FROM feature_delays")
        inserted_count = cursor.fetchone()[0]
        logger.info(f"Successfully inserted or verified {inserted_count} rows")
        return {'rows_inserted': inserted_count}
    except Exception as e:
        logger.error(f'Error in load_features: {str(e)}')
        raise
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()