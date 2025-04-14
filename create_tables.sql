-- Staging Table
CREATE TABLE IF NOT EXISTS staging_weather (
    staging_id SERIAL PRIMARY KEY,
    raw_json JSONB NOT NULL,
    base_date DATE,
    base_time VARCHAR(4),
    nx INTEGER,
    ny INTEGER,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed BOOLEAN DEFAULT FALSE
);


-- create tables 
CREATE TABLE IF NOT EXISTS dim_date (
    date_id SERIAL PRIMARY KEY,
    base_date DATE NOT NULL,
    year INTEGER,
    month SMALLINT,
    day SMALLINT,
    day_of_week TEXT,
    is_holiday TEXT
);

CREATE TABLE IF NOT EXISTS dim_time (
    time_id SERIAL PRIMARY KEY,
    base_time VARCHAR(4) NOT NULL,
    hour SMALLINT
);

CREATE TABLE IF NOT EXISTS dim_location (
    location_id SERIAL PRIMARY KEY,
    nx INTEGER NOT NULL,
    ny INTEGER NOT NULL,
    admin_district_code TEXT,
    city TEXT,
    sub_address TEXT,
    effective_date DATE NOT NULL,
    expiration_date DATE,
    is_current BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS dim_category (
    category_id SERIAL PRIMARY KEY,
    category_code TEXT NOT NULL,
    category_description TEXT,
    unit TEXT,
    effective_date DATE NOT NULL,
    expiration_date DATE,
    is_current BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS fact_weather_measurement (
    date_id INTEGER NOT NULL,
    time_id INTEGER NOT NULL,
    location_id INTEGER NOT NULL,
    category_id INTEGER NOT NULL,
    measurement_value DECIMAL(10,2) NOT NULL,
    PRIMARY KEY (date_id, time_id, location_id, category_id),
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
    FOREIGN KEY (time_id) REFERENCES dim_time(time_id),
    FOREIGN KEY (location_id) REFERENCES dim_location(location_id),
    FOREIGN KEY (category_id) REFERENCES dim_category(category_id)
);

CREATE TABLE if NOT EXISTS feature_delays (
    timestamp TIMESTAMP NOT NULL, 
    city TEXT NOT NULL,     
    day INTEGER,                           
    hour INTEGER,                         
    sin_hour DOUBLE PRECISION,                        
    cos_hour DOUBLE PRECISION,                       
    is_weekend INTEGER,                   
    day_of_week_encoded SMALLINT,          
    PTY DOUBLE PRECISION,                             -- rainfall type
    REH DOUBLE PRECISION,                             -- Relative humidity
    RN1 DOUBLE PRECISION,                             -- Rainfall amount
    T1H DOUBLE PRECISION,                             -- Temperature
    WSD DOUBLE PRECISION,                             -- Wind speed
    PTY_lag1 DOUBLE PRECISION,                        -- PTY from 1 hour ago
    PTY_lag2 DOUBLE PRECISION,                        -- PTY from 2 hours ago
    delay_hours_lag1 DOUBLE PRECISION,                -- delay_hours from 1 hour ago
    delay_hours_lag2 DOUBLE PRECISION,                -- delay_hours from 2 hours ago
    delay_hours DOUBLE PRECISION,                     -- Target variable
    PRIMARY KEY (timestamp, city)                    
);