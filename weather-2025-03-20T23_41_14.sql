
CREATE TABLE dim_category
(
  category_id          INTEGER NOT NULL DEFAULT 1 COMMENT 'PK',
  category_code        TEXT    NULL     COMMENT 'natural key',
  category_description TEXT    NULL    ,
  unit                 TEXT    NULL    ,
  effective_date       DATE    NULL    ,
  experation_date      DATE    NULL    ,
  is_current           BOOLEAN NULL    ,
  PRIMARY KEY (category_id)
);

CREATE TABLE dim_date
(
  date_id     INTEGER NOT NULL DEFAULT 1 COMMENT 'PK',
  base_date   NUMERIC NULL    ,
  year        NUMERIC NULL    ,
  month       NUMERIC NULL    ,
  day         NUMERIC NULL    ,
  day_of_week TEXT    NULL    ,
  is_holiday  TEXT    NULL     DEFAULT Holiday or Non-Holiday,
  PRIMARY KEY (date_id)
);

CREATE TABLE dim_location
(
  location_id         INTEGER NOT NULL DEFAULT 1 COMMENT 'PK',
  nx                  INTEGER NULL    ,
  ny                  INTEGER NULL    ,
  admin_district_code TEXT    NULL     COMMENT 'natural key',
  city                TEXT    NULL    ,
  sub_address         TEXT    NULL    ,
  effective_date      DATE    NULL    ,
  experation_date     DATE    NULL    ,
  is_current          BOOLEAN NULL    ,
  PRIMARY KEY (location_id)
);

CREATE TABLE dim_time
(
  time_id   INTEGER NOT NULL DEFAULT 1 COMMENT 'PK',
  base_time NUMERIC NULL    ,
  hour      NUMERIC NULL    ,
  PRIMARY KEY (time_id)
);

CREATE TABLE fact_weather_measurement
(
  measurement_id    INTEGER NOT NULL DEFAULT 1 COMMENT 'PK',
  date_id           INTEGER NULL     COMMENT 'FK',
  time_id           INTEGER NULL     COMMENT 'FK',
  location_id       INTEGER NULL     COMMENT 'FK',
  category_id       INTEGER NULL     COMMENT 'FK',
  measurement_value FLOAT   NULL    
);

ALTER TABLE fact_weather_measurement
  ADD CONSTRAINT FK_dim_time_TO_fact_weather_measurement
    FOREIGN KEY (time_id)
    REFERENCES dim_time (time_id);

ALTER TABLE fact_weather_measurement
  ADD CONSTRAINT FK_dim_date_TO_fact_weather_measurement
    FOREIGN KEY (date_id)
    REFERENCES dim_date (date_id);

ALTER TABLE fact_weather_measurement
  ADD CONSTRAINT FK_dim_location_TO_fact_weather_measurement
    FOREIGN KEY (location_id)
    REFERENCES dim_location (location_id);

ALTER TABLE fact_weather_measurement
  ADD CONSTRAINT FK_dim_category_TO_fact_weather_measurement
    FOREIGN KEY (category_id)
    REFERENCES dim_category (category_id);
