-- =====================================================
-- Dimensional Model DDL - Peru Air Quality
-- Purpose: Create star schema for OLAP-style analytics
-- Dependencies:
--   - dev_db.consumption_sch.peru_aqi_consumption_dt (from 03-consumption-layer.sql)
--
-- Object Creation Order:
--   1. Dimension Table: peru_date_dim
--   2. Dimension Table: peru_location_dim
--   3. Fact Table: peru_air_quality_fact
--
-- Schema Type: Star Schema
-- Fact Grain: One row per measurement per location per timestamp
-- Version: 1.0.0
-- =====================================================

USE ROLE accountadmin;
USE SCHEMA dev_db.consumption_sch;
USE WAREHOUSE adhoc_wh;

-- =====================================================
-- 1. DIMENSION TABLE: peru_date_dim
-- Purpose: Time dimension for temporal analysis
-- Grain: One row per unique timestamp
-- Key: HASH(measurement_time)
-- =====================================================

CREATE OR REPLACE DYNAMIC TABLE peru_date_dim
    TARGET_LAG = 'DOWNSTREAM'
    WAREHOUSE = transform_wh
    COMMENT = 'Date dimension for Peru air quality - supports time-based analysis'
AS
WITH time_data AS (
    SELECT 
        aqi_timestamp AS measurement_time,
        aqi_year, aqi_month, aqi_quarter, aqi_day, aqi_hour,
        aqi_day_of_week, aqi_day_name
    FROM dev_db.consumption_sch.peru_aqi_consumption_dt
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
)
SELECT 
    HASH(measurement_time) AS date_pk,
    measurement_time, aqi_year, aqi_month, aqi_quarter, 
    aqi_day, aqi_hour, aqi_day_of_week, aqi_day_name
FROM time_data;

-- =====================================================
-- 2. DIMENSION TABLE: peru_location_dim
-- Purpose: Geographic dimension for spatial analysis
-- Grain: One row per unique (latitude, longitude)
-- Key: HASH(latitude, longitude)
-- =====================================================

CREATE OR REPLACE DYNAMIC TABLE peru_location_dim
    TARGET_LAG = 'DOWNSTREAM'
    WAREHOUSE = transform_wh
    COMMENT = 'Location dimension for Peru air quality - supports geographic analysis'
AS
WITH location_data AS (
    SELECT 
        latitude, longitude, country, city, district,
        location_name, region, timezone_id
    FROM dev_db.consumption_sch.peru_aqi_consumption_dt
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
)
SELECT 
    HASH(latitude, longitude) AS location_pk,
    latitude, longitude, country, city, district,
    location_name, region, timezone_id
FROM location_data;

-- =====================================================
-- 3. FACT TABLE: peru_air_quality_fact
-- Purpose: Measurement facts with foreign keys to dimensions
-- Grain: One row per measurement per location per timestamp
-- Keys: 
--   - Primary: HASH(timestamp, latitude, longitude)
--   - Foreign: date_fk (→ peru_date_dim), location_fk (→ peru_location_dim)
-- =====================================================

CREATE OR REPLACE DYNAMIC TABLE peru_air_quality_fact
    TARGET_LAG = 'DOWNSTREAM'
    WAREHOUSE = transform_wh
    COMMENT = 'Fact table for Peru air quality measurements - grain: one row per measurement per location per timestamp'
AS
SELECT 
    -- Primary key
    HASH(aqi_timestamp, latitude, longitude) AS aqi_pk,
    
    -- Foreign keys
    HASH(aqi_timestamp) AS date_fk,
    HASH(latitude, longitude) AS location_fk,
    
    -- Air Quality Facts
    pm2_5, pm10, so2, no2, co, o3, us_epa_index, gb_defra_index,
    
    -- Calculated Metrics
    prominent_pollutant, criteria_pollutant_count, custom_aqi,
    
    -- Weather Context
    temp_c, temp_f, humidity, cloud_cover, wind_kph, wind_direction,
    pressure_mb, precip_mm, visibility_km, uv_index,
    
    -- Data Quality
    is_valid_measurement,
    
    -- Metadata
    _stg_file_name, _copy_data_ts
    
FROM dev_db.consumption_sch.peru_aqi_consumption_dt;

-- Script execution completed
SELECT 'Dimensional model created successfully' AS status;
