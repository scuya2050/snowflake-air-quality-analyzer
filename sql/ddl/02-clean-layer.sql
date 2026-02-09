-- =====================================================
-- Clean Layer DDL - Peru Air Quality
-- Purpose: Create clean layer dynamic table with deduplication
-- Dependencies: 
--   - dev_db.stage_sch.raw_aqi (from 01-stage-layer.sql)
--   - dev_db.clean_sch schema (created via Terraform)
--   - transform_wh warehouse (created via Terraform)
--
-- Object Creation Order:
--   1. Dynamic Table: clean_peru_aqi_dt
--
-- Version: 1.0.0
-- =====================================================

USE ROLE accountadmin;
USE SCHEMA dev_db.clean_sch;
USE WAREHOUSE adhoc_wh;

-- =====================================================
-- 1. DYNAMIC TABLE: CLEAN PERU AQI
-- Purpose: Deduplicated air quality + weather data
-- Grain: One row per unique (timestamp, location) combination
-- Deduplication: Latest file wins when duplicates exist
-- =====================================================

CREATE OR REPLACE DYNAMIC TABLE dev_db.clean_sch.clean_peru_aqi_dt
    TARGET_LAG = 'downstream'
    WAREHOUSE = transform_wh
    COMMENT = 'v1.0.0 - Deduplicated and transformed Peru air quality data from Weather API'
AS
WITH peru_aqi_with_rank AS (
    SELECT 
        -- Measurement identifier
        raw:current:last_updated::VARCHAR AS measurement_ts,
        raw:current:last_updated_epoch::NUMBER AS measurement_epoch,
        
        -- Location information
        raw:location:name::VARCHAR AS location_name,
        raw:location:region::VARCHAR AS region,
        raw:location:country::VARCHAR AS location_country,
        raw:location:lat::NUMBER(10,7) AS latitude,
        raw:location:lon::NUMBER(10,7) AS longitude,
        raw:location:tz_id::VARCHAR AS timezone_id,
        raw:location:localtime::VARCHAR AS local_time,
        raw:location:localtime_epoch::NUMBER AS local_time_epoch,
        
        -- Air Quality Metrics
        raw:current:air_quality:co::NUMBER(10,2) AS co,
        raw:current:air_quality:no2::NUMBER(10,2) AS no2,
        raw:current:air_quality:o3::NUMBER(10,2) AS o3,
        raw:current:air_quality:pm10::NUMBER(10,2) AS pm10,
        raw:current:air_quality:pm2_5::NUMBER(10,2) AS pm2_5,
        raw:current:air_quality:so2::NUMBER(10,2) AS so2,
        raw:current:air_quality:"us-epa-index"::NUMBER AS us_epa_index,
        raw:current:air_quality:"gb-defra-index"::NUMBER AS gb_defra_index,
        
        -- Weather Metrics (contextual data)
        raw:current:temp_c::NUMBER(10,2) AS temp_c,
        raw:current:temp_f::NUMBER(10,2) AS temp_f,
        raw:current:humidity::NUMBER AS humidity,
        raw:current:cloud::NUMBER AS cloud_cover,
        raw:current:wind_kph::NUMBER(10,2) AS wind_kph,
        raw:current:wind_mph::NUMBER(10,2) AS wind_mph,
        raw:current:wind_degree::NUMBER AS wind_degree,
        raw:current:wind_dir::VARCHAR AS wind_direction,
        raw:current:pressure_mb::NUMBER(10,2) AS pressure_mb,
        raw:current:precip_mm::NUMBER(10,2) AS precip_mm,
        raw:current:vis_km::NUMBER(10,2) AS visibility_km,
        raw:current:uv::NUMBER AS uv_index,
        raw:current:condition:text::VARCHAR AS weather_condition,
        raw:current:condition:code::NUMBER AS weather_condition_code,
        
        -- Partition attributes from file path
        country,
        city,
        district,
        year,
        month,
        day,
        
        -- Metadata columns for traceability
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        _copy_data_ts,
        _copy_data_user,
        _copy_data_role,
        
        -- Deduplication logic: 
        -- Partition by unique measurement (timestamp + location)
        -- Order by file load time (latest file wins)
        ROW_NUMBER() OVER (
            PARTITION BY 
                raw:current:last_updated::VARCHAR,
                raw:location:name::VARCHAR,
                raw:location:lat::NUMBER(10,7),
                raw:location:lon::NUMBER(10,7)
            ORDER BY _stg_file_load_ts DESC
        ) AS latest_file_rank
        
    FROM dev_db.stage_sch.raw_aqi
    WHERE country = 'peru'
        AND raw:current:last_updated IS NOT NULL
)
SELECT 
    measurement_ts,
    measurement_epoch,
    location_name,
    region,
    location_country,
    latitude,
    longitude,
    timezone_id,
    local_time,
    local_time_epoch,
    co,
    no2,
    o3,
    pm10,
    pm2_5,
    so2,
    us_epa_index,
    gb_defra_index,
    temp_c,
    temp_f,
    humidity,
    cloud_cover,
    wind_kph,
    wind_mph,
    wind_degree,
    wind_direction,
    pressure_mb,
    precip_mm,
    visibility_km,
    uv_index,
    weather_condition,
    weather_condition_code,
    country,
    city,
    district,
    year,
    month,
    day,
    _stg_file_name,
    _stg_file_load_ts,
    _stg_file_md5,
    _copy_data_ts,
    _copy_data_user,
    _copy_data_role
FROM peru_aqi_with_rank
WHERE latest_file_rank = 1;

-- Script execution completed
SELECT 'Clean layer dynamic table created successfully' AS status;
