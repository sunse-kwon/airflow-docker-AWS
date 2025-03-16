from airflow.hooks.postgres_hook import PostgresHook



def stage_weather_data(ti):
    """Load weather data into PostgreSQL using PostgresHook."""
    # Pull the processed data from XCom
    weather_data = ti.xcom_pull(task_ids='process_weather_data')
    if not weather_data:
        raise ValueError("No weather data received from process_task")

    # Initialize PostgresHook with your connection ID
    pg_hook = PostgresHook(postgres_conn_id='weather_connection')
    
    pg_hook.insert_rows(
        table="staging_weather",
        rows=weather_data,
        target_fields=["raw_json", "base_date", "base_time", "nx", "ny"],
        replace=False
    )
    # Execute the INSERT query with the data
    # for data in weather_data:
    #     pg_hook.run(
    #         sql="""
    #         INSERT INTO staging_weather (raw_json, base_date, base_time, nx, ny)
    #         VALUES (%s, %s, %s, %s, %s)
    #         ON CONFLICT ON CONSTRAINT unique_staging_weather DO NOTHING
    #         """,
    #         parameters=data,
          
    #         )