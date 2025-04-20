from airflow.hooks.base import BaseHook
import logging


logger = logging.getLogger(__name__)

# Define a function to extract data with column names
def extract_data_with_columns(ti):
    # Get the connection
    conn_id = 'weather_connection'
    hook = BaseHook.get_hook(conn_id)
    
    # Run the query
    sql = "SELECT * FROM feature_delays"
    result = hook.get_pandas_df(sql)  # Use pandas for simplicity
    logger.info(f'result : {result}')
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