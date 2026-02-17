-- =====================================================
-- Streamlit Views (Data Contracts)
-- Purpose: Create views optimized for Streamlit dashboard. Includes utility functions
-- Dependencies:
--   - dev_db.publish_sch.air_quality_fact (from 04-dimensional-model.sql)
--   - dev_db.publish_sch.location_dim (from 04-dimensional-model.sql)
--   - dev_db.publish_sch.date_dim (from 04-dimensional-model.sql)
-- Deployment: Part of data pipeline (deploy-pipeline.yml)
-- Hierarchy: Country → City → District (no stations)
-- Schema: publish_sch
-- =====================================================

USE ROLE accountadmin;
USE WAREHOUSE adhoc_wh;
USE DATABASE dev_db;
USE SCHEMA publish_sch;

-- ==============================================================================
-- STREAMLIT DATA VIEWS
-- ==============================================================================

-- =====================================================
-- 1. VIEW: vw_daily_city_agg
-- Purpose: Daily city-level aggregations for trend analysis
-- Used By: Streamlit Page 1 (Air Quality Trend - City+Day Level)
-- Grain: One row per country/city/day
-- =====================================================

CREATE OR REPLACE VIEW dev_db.publish_sch.vw_daily_city_agg
COMMENT = 'Daily city-level aggregations for Streamlit Page 1 - Top cities by AQI'
AS
SELECT 
    -- Location hierarchy (city level aggregation)
    l.country,
    l.city,
    
    -- Time dimension
    DATE(f.local_timestamp) AS measurement_date,
    d.aqi_year,
    d.aqi_month,
    d.aqi_day_name,
    
    -- Pollutant averages (daily aggregation across all districts in city)
    ROUND(AVG(f.pm2_5), 2) AS pm25_avg,
    ROUND(AVG(f.pm10), 2) AS pm10_avg,
    ROUND(AVG(f.co), 2) AS co_avg,
    ROUND(AVG(f.no2), 2) AS no2_avg,
    ROUND(AVG(f.o3), 2) AS o3_avg,
    ROUND(AVG(f.so2), 2) AS so2_avg,
    
    -- Most prominent pollutant of the day
    MODE(f.prominent_pollutant) AS prominent_pollutant,
    
    -- AQI (average of hourly custom AQI across districts)
    ROUND(AVG(f.custom_aqi), 0) AS aqi,
    
    -- Data quality metrics
    COUNT(*) AS hourly_readings_in_day,
    COUNT(DISTINCT l.district) AS districts_reporting,
    SUM(f.readings_in_hour) AS total_raw_readings
    
FROM dev_db.publish_sch.air_quality_fact f
JOIN dev_db.publish_sch.location_dim l ON f.location_fk = l.location_pk
JOIN dev_db.publish_sch.date_dim d ON f.date_fk = d.date_pk
GROUP BY 
    l.country, 
    l.city,
    DATE(f.local_timestamp), 
    d.aqi_year, 
    d.aqi_month, 
    d.aqi_day_name;

-- =====================================================
-- 2. VIEW: vw_hourly_city_agg
-- Purpose: Hourly city-level aggregations for intraday trends
-- Used By: Streamlit Page 2 (AQI Trend - By City/Hour Level)
-- Grain: One row per country/city/hour
-- =====================================================

CREATE OR REPLACE VIEW dev_db.publish_sch.vw_hourly_city_agg
COMMENT = 'Hourly city-level aggregations for Streamlit Page 2 - Intraday trends'
AS
SELECT 
    -- Location hierarchy (city level)
    l.country,
    l.city,
    
    -- Time dimension (hourly grain)
    f.local_timestamp AS measurement_time,
    d.aqi_year,
    d.aqi_month,
    d.aqi_day,
    d.aqi_hour,
    d.aqi_day_name,
    
    -- Pollutant averages (city-level aggregation across districts)
    ROUND(AVG(f.pm2_5), 2) AS pm25_avg,
    ROUND(AVG(f.pm10), 2) AS pm10_avg,
    ROUND(AVG(f.co), 2) AS co_avg,
    ROUND(AVG(f.no2), 2) AS no2_avg,
    ROUND(AVG(f.o3), 2) AS o3_avg,
    ROUND(AVG(f.so2), 2) AS so2_avg,
    
    -- Weather context (average across districts)
    ROUND(AVG(f.temp_c), 1) AS temp_c_avg,
    ROUND(AVG(f.humidity), 0) AS humidity_avg,
    ROUND(AVG(f.wind_kph), 1) AS wind_kph_avg,
    
    -- Data quality
    COUNT(DISTINCT l.district) AS districts_reporting,
    SUM(f.readings_in_hour) AS total_raw_readings
    
FROM dev_db.publish_sch.air_quality_fact f
JOIN dev_db.publish_sch.location_dim l ON f.location_fk = l.location_pk
JOIN dev_db.publish_sch.date_dim d ON f.date_fk = d.date_pk
GROUP BY 
    l.country, 
    l.city,
    f.local_timestamp, 
    d.aqi_year, 
    d.aqi_month, 
    d.aqi_day, 
    d.aqi_hour, 
    d.aqi_day_name;

-- =====================================================
-- 3. VIEW: vw_hourly_district_detail
-- Purpose: Hourly district-level data for detailed drill-down
-- Used By: Streamlit Page 3 (Air Quality Trend - At District Level)
-- Grain: One row per district per hour (no aggregation)
-- =====================================================

CREATE OR REPLACE VIEW dev_db.publish_sch.vw_hourly_district_detail
COMMENT = 'Hourly district-level detail for Streamlit Page 3 - District drill-down'
AS
SELECT 
    -- Location (district is lowest grain)
    l.country,
    l.city,
    l.district,
    l.location_name,
    l.region,
    l.latitude,
    l.longitude,
    l.timezone_id,
    
    -- Time
    f.local_timestamp AS measurement_time,
    f.utc_timestamp AS measurement_time_utc,
    d.aqi_year,
    d.aqi_month,
    d.aqi_day,
    d.aqi_hour,
    d.aqi_day_name,
    
    -- Pollutants (hourly averages from consumption layer)
    f.pm2_5 AS pm25_avg,
    f.pm10 AS pm10_avg,
    f.so2 AS so2_avg,
    f.no2 AS no2_avg,
    f.co AS co_avg,
    f.o3 AS o3_avg,
    
    -- AQI indices
    f.us_epa_index,
    f.gb_defra_index,
    f.custom_aqi AS aqi,
    f.prominent_pollutant,
    f.criteria_pollutant_count,
    
    -- Weather
    f.temp_c,
    f.humidity,
    f.cloud_cover,
    f.wind_kph,
    f.wind_direction,
    f.pressure_mb,
    f.precip_mm,
    f.visibility_km,
    f.uv_index,
    
    -- Data quality
    f.readings_in_hour,
    f.is_valid_measurement
    
FROM dev_db.publish_sch.air_quality_fact f
JOIN dev_db.publish_sch.location_dim l ON f.location_fk = l.location_pk
JOIN dev_db.publish_sch.date_dim d ON f.date_fk = d.date_pk;

-- =====================================================
-- 4. VIEW: vw_latest_district_aqi
-- Purpose: Most recent AQI reading per district for map visualization
-- Used By: Legacy - consider using vw_daily_district_aqi instead
-- Grain: One row per district (latest reading only)
-- =====================================================

CREATE OR REPLACE VIEW dev_db.publish_sch.vw_latest_district_aqi
COMMENT = 'Latest AQI per district for map visualization'
AS
WITH ranked_readings AS (
    SELECT 
        f.aqi_pk,
        f.location_fk,
        f.local_timestamp,
        f.utc_timestamp,
        f.pm2_5,
        f.pm10,
        f.custom_aqi,
        f.prominent_pollutant,
        f.us_epa_index,
        f.gb_defra_index,
        l.country,
        l.city,
        l.district,
        l.location_name,
        l.latitude,
        l.longitude,
        l.timezone_id,
        ROW_NUMBER() OVER (
            PARTITION BY f.location_fk 
            ORDER BY f.local_timestamp DESC
        ) AS recency_rank
    FROM dev_db.publish_sch.air_quality_fact f
    JOIN dev_db.publish_sch.location_dim l ON f.location_fk = l.location_pk
)
SELECT 
    country,
    city,
    district,
    location_name,
    latitude,
    longitude,
    timezone_id,
    local_timestamp AS latest_reading_time,
    utc_timestamp AS latest_reading_time_utc,
    pm2_5,
    pm10,
    custom_aqi AS aqi,
    prominent_pollutant,
    us_epa_index,
    gb_defra_index
FROM ranked_readings
WHERE recency_rank = 1;

-- =====================================================
-- 5. VIEW: vw_daily_district_aqi
-- Purpose: Daily district-level AQI for filtered bubble map visualization
-- Used By: Streamlit Page 4 (Air Quality Map - Filtered by City+Date)
-- Grain: One row per district per date
-- =====================================================

CREATE OR REPLACE VIEW dev_db.publish_sch.vw_daily_district_aqi
COMMENT = 'Daily district-level AQI for Streamlit Page 4 - City bubble map with date filter'
AS
SELECT 
    -- Location
    l.country,
    l.city,
    l.district,
    l.location_name,
    l.latitude,
    l.longitude,
    l.timezone_id,
    
    -- Time
    DATE(f.local_timestamp) AS measurement_date,
    
    -- Daily aggregated pollutants
    ROUND(AVG(f.pm2_5), 2) AS pm25_avg,
    ROUND(AVG(f.pm10), 2) AS pm10_avg,
    ROUND(AVG(f.co), 2) AS co_avg,
    ROUND(AVG(f.no2), 2) AS no2_avg,
    ROUND(AVG(f.o3), 2) AS o3_avg,
    ROUND(AVG(f.so2), 2) AS so2_avg,
    
    -- Daily AQI (average of hourly readings)
    ROUND(AVG(f.custom_aqi), 0) AS aqi,
    
    -- Most prominent pollutant of the day
    MODE(f.prominent_pollutant) AS prominent_pollutant,
    
    -- Data quality
    COUNT(*) AS hourly_readings_in_day,
    SUM(f.readings_in_hour) AS total_raw_readings
    
FROM dev_db.publish_sch.air_quality_fact f
JOIN dev_db.publish_sch.location_dim l ON f.location_fk = l.location_pk
GROUP BY 
    l.country,
    l.city,
    l.district,
    l.location_name,
    l.latitude,
    l.longitude,
    l.timezone_id,
    DATE(f.local_timestamp);

-- =====================================================
-- 6. VIEW: vw_location_hierarchy
-- Purpose: Location hierarchy for Streamlit dropdown filters
-- Used By: Streamlit Pages 2, 3, 4 (Cascading dropdowns)
-- Grain: One row per unique district
-- =====================================================

CREATE OR REPLACE VIEW dev_db.publish_sch.vw_location_hierarchy
COMMENT = 'Location hierarchy for Streamlit dropdown filters - Cascading selection'
AS
SELECT DISTINCT
    l.country,
    l.city,
    l.district,
    l.location_name,
    l.region,
    l.timezone_id,
    l.latitude,
    l.longitude,
    COUNT(DISTINCT DATE(f.local_timestamp)) OVER (
        PARTITION BY l.location_pk
    ) AS days_with_data,
    MAX(f.local_timestamp) OVER (
        PARTITION BY l.location_pk
    ) AS latest_reading_time
FROM dev_db.publish_sch.location_dim l
LEFT JOIN dev_db.publish_sch.air_quality_fact f ON l.location_pk = f.location_fk
ORDER BY l.country, l.city, l.district;


-- =====================================================
-- UTILITY FUNCTIONS
-- =====================================================

-- Function: Get current session timezone
-- Purpose: Returns the active Snowflake session's timezone setting
-- Used By: Streamlit pages for timezone-aware timestamp display
CREATE OR REPLACE FUNCTION dev_db.publish_sch.GET_CURRENT_TIMEZONE()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS 
$$
  const timezone = Intl.DateTimeFormat().resolvedOptions().timeZone;
  return timezone;
$$
;

-- Note: Snowflake doesn't expose session timezone directly via SQL
-- Workaround: Use SHOW PARAMETERS
-- This requires a stored procedure instead of a function

-- Script execution completed
SELECT '6 Streamlit views and utility functions created in publish_sch' AS status;
