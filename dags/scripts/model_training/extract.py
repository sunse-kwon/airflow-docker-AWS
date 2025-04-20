import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

import logging


logger = logging.getLogger(__name__)

# Define a function to extract data with column names
def extract_data_with_columns(ti):
    # Get the connection
    
    hook = PostgresHook(postgres_conn_id='weather_connection')
    
    # Run the query
    sql = "SELECT * FROM feature_delays"
    try:
        result = hook.get_pandas_df(sql)
    except Exception as e:
        logger.error(f'Failed to execute query: {str(e)}')
        raise ValueError(f'Failed to execute query: {str(e)}')
    
    # Convert Timestamp or other non-serializable columns to strings
    for col in result.columns:
        if pd.api.types.is_datetime64_any_dtype(result[col]):
            result[col] = result[col].astype(str)

    logger.info(f'result converted: {result}')
    # Alternatively, use raw cursor for more control
    # conn = hook.get_conn()
    # cursor = conn.cursor()
    # cursor.execute(sql)
    # rows = cursor.fetchall()
    # columns = [desc[0] for desc in cursor.description]
    # cursor.close()
    # conn.close()
    # result = {'columns': columns, 'data': rows}
    
    # Push results to XCom
    ti.xcom_push(key='query_results', value={
        'columns': list(result.columns),
        'data': result.to_dict(orient='records')
    })