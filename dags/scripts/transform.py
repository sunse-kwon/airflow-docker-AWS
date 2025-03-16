# Process weather data for SQL insertion
def process_weather_data(ti):
    # Fetch data from XCom
    weather_data = ti.xcom_pull(task_ids='fetch_weather_data')
    # weather_data is a list of tuples: [(raw_json, base_date, base_time, nx, ny), ...]
    
    # Prepare data for bulk insertion
    processed_data = []
    for item in weather_data:
        raw_json, base_date, base_time, nx, ny = item
        # Convert date and time to strings, escape quotes in raw_json
        processed_row = (
            str(raw_json).replace("'", "''"),  # Escape single quotes for SQL
            base_date.strftime('%Y-%m-%d'),    # Convert date to string
            base_time,    
            nx,                                # Integer
            ny                                 # Integer
        )
        processed_data.append(processed_row)
    
    return processed_data  # List of tuples for bulk insertion