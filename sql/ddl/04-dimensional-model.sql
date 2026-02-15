-- =====================================================
-- Dimensional Model DDL - Air Quality (Multi-Country)
-- Purpose: Create star schema for OLAP-style analytics
-- Dependencies:
--   - dev_db.consumption_sch.aqi_consumption_dt (from 03-consumption-layer.sql)
--
-- Object Creation Order:
--   1. Dimension Table: date_dim
--   2. Dimension Table: location_dim
--   3. Fact Table: air_quality_fact
--
-- Schema Type: Star Schema
-- Fact Grain: One row per HOUR per location (consumption layer has hourly aggregation)
-- Time Basis: Local Time (enables cross-country time-of-day analysis)
-- Multi-Country Support: Ready for Peru, Singapore, India, etc.
-- Version: 2.0.0
-- =====================================================

USE ROLE accountadmin;
USE SCHEMA dev_db.publish_sch;
USE WAREHOUSE adhoc_wh;

-- =====================================================
-- 1. DIMENSION TABLE: date_dim
-- Purpose: Local time dimension for time-of-day analysis
-- Grain: One row per unique hourly local timestamp
-- Key: HASH(local_timestamp)
-- Use Case: Compare all countries at same local hour (e.g., 3 PM everywhere)
-- =====================================================

CREATE OR REPLACE DYNAMIC TABLE dev_db.publish_sch.date_dim
    TARGET_LAG = 'DOWNSTREAM'
    WAREHOUSE = transform_wh
    COMMENT = 'Local time dimension for cross-country time-of-day analysis'
AS
WITH time_data AS (
    SELECT 
        aqi_timestamp AS local_timestamp,
        aqi_year, aqi_month, aqi_quarter, aqi_day, aqi_hour,
        aqi_day_of_week, aqi_day_name
    FROM dev_db.consumption_sch.aqi_consumption_dt
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
)
SELECT 
    HASH(local_timestamp) AS date_pk,
    local_timestamp, aqi_year, aqi_month, aqi_quarter, 
    aqi_day, aqi_hour, aqi_day_of_week, aqi_day_name
FROM time_data;

-- =====================================================
-- 2. DIMENSION TABLE: location_dim
-- Purpose: Geographic dimension for spatial analysis
-- Grain: One row per unique (latitude, longitude)
-- Key: HASH(latitude, longitude)
-- Multi-Country: Contains timezone_id for local time conversion
-- =====================================================

CREATE OR REPLACE DYNAMIC TABLE dev_db.publish_sch.location_dim
    TARGET_LAG = 'DOWNSTREAM'
    WAREHOUSE = transform_wh
    COMMENT = 'Location dimension for multi-country air quality - supports geographic and timezone analysis'
AS
WITH location_data AS (
    SELECT 
        latitude, longitude, country, city, district,
        location_name, region, timezone_id
    FROM dev_db.consumption_sch.aqi_consumption_dt
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
)
SELECT 
    HASH(latitude, longitude) AS location_pk,
    latitude, longitude, country, city, district,
    location_name, region, timezone_id
FROM location_data;

-- =====================================================
-- 3. FACT TABLE: air_quality_fact
-- Purpose: Hourly aggregated measurement facts with foreign keys to dimensions
-- Grain: One row per HOUR per location (matches consumption layer grain)
-- Keys: 
--   - Primary: HASH(local_timestamp, latitude, longitude)
--   - Foreign: date_fk (→ date_dim), location_fk (→ location_dim)
-- Time Basis: Local time for meaningful cross-country comparisons
-- Data Quality: Includes readings_in_hour to track aggregation quality
-- =====================================================

CREATE OR REPLACE DYNAMIC TABLE dev_db.publish_sch.air_quality_fact
    TARGET_LAG = 'DOWNSTREAM'
    WAREHOUSE = transform_wh
    COMMENT = 'Fact table for multi-country air quality - hourly grain with local time for time-of-day analysis'
AS
SELECT 
    -- Primary key
    HASH(aqi_timestamp, latitude, longitude) AS aqi_pk,
    
    -- Foreign keys
    HASH(aqi_timestamp) AS date_fk,
    HASH(latitude, longitude) AS location_fk,
    
    -- Time attributes (denormalized for fast filtering)
    aqi_timestamp AS local_timestamp,
    aqi_timestamp_utc AS utc_timestamp,
    aqi_hour,
    aqi_day_of_week,
    aqi_day_name,
    
    -- Air Quality Facts (hourly averages)
    pm2_5, pm10, so2, no2, co, o3, us_epa_index, gb_defra_index,
    
    -- Calculated Metrics (applied to hourly averages)
    prominent_pollutant, criteria_pollutant_count, custom_aqi,
    
    -- Weather Context (hourly averages)
    temp_c, humidity, cloud_cover, wind_kph, wind_direction,
    pressure_mb, precip_mm, visibility_km, uv_index,
    
    -- Data Quality
    is_valid_measurement,
    readings_in_hour,  -- Number of raw readings averaged into this hour
    
    -- Metadata
    _stg_file_load_ts, _copy_data_ts
    
FROM dev_db.consumption_sch.aqi_consumption_dt;

-- Script execution completed
SELECT 'Dimensional model created successfully' AS status;
