-- =====================================================
-- Consumption Layer DDL - Air Quality (Multi-Country)
-- Purpose: Create consumption layer with UDFs, dynamic table, and analytical views
-- Dependencies:
--   - dev_db.clean_sch.clean_aqi_dt (from 02-clean-layer.sql)
--   - dev_db.consumption_sch schema (created via Terraform)
--   - transform_wh warehouse (created via Terraform)
--
-- Object Creation Order:
--   1. UDF: prominent_pollutant
--   2. UDF: aqi_criteria_met
--   3. UDF: calculate_custom_aqi
--   4. Dynamic Table: aqi_consumption_dt (hourly aggregation)
--   5. View: vw_aqi_daily
--   6. View: vw_aqi_current
--
-- Multi-Country Support: Works for Peru, Singapore, India, etc.
-- Version: 2.0.0
-- =====================================================

USE ROLE accountadmin;
USE SCHEMA dev_db.consumption_sch;
USE WAREHOUSE adhoc_wh;

-- =====================================================
-- USER-DEFINED FUNCTIONS (UDFs)
-- Purpose: Business logic functions for air quality calculations
-- =====================================================

-- -----------------------------------------------------
-- 0. UDF: format_location_name
-- Purpose: Formats location names to proper title case with Spanish article rules
-- Returns: Properly formatted location name
-- Examples: 
--   santa_anita → Santa Anita
--   san_juan_de_lurigancho → San Juan de Lurigancho
--   new_delhi → New Delhi
-- -----------------------------------------------------
CREATE OR REPLACE FUNCTION format_location_name(name VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.12'
HANDLER = 'format_location_name'
COMMENT = 'Formats location names with proper title case and Spanish article rules'
AS $$
def format_location_name(name):
    if name is None:
        return None
    
    # Replace underscores with spaces
    name = name.replace('_', ' ')
    
    # Spanish articles and prepositions that should remain lowercase (except first word)
    lowercase_words = {'de', 'del', 'la', 'las', 'el', 'los', 'y', 'e', 'a', 'al'}
    
    # Split into words and apply title case rules
    words = name.split()
    formatted_words = []
    
    for i, word in enumerate(words):
        # First word is always title case, others check if they are articles
        if i == 0 or word.lower() not in lowercase_words:
            formatted_words.append(word.capitalize())
        else:
            formatted_words.append(word.lower())
    
    return ' '.join(formatted_words)
$$;

-- -----------------------------------------------------
-- 1. UDF: prominent_pollutant
-- Purpose: Identifies the pollutant with highest concentration
-- Returns: Pollutant name (PM2.5, PM10, SO2, NO2, CO, O3)
-- -----------------------------------------------------
CREATE OR REPLACE FUNCTION prominent_pollutant(
    pm25 NUMBER, pm10 NUMBER, so2 NUMBER, no2 NUMBER, co NUMBER, o3 NUMBER
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.12'
HANDLER = 'prominent_pollutant'
COMMENT = 'Returns the pollutant with the highest concentration value'
AS $$
def prominent_pollutant(pm25, pm10, so2, no2, co, o3):
    pm25 = pm25 if pm25 is not None else 0
    pm10 = pm10 if pm10 is not None else 0
    so2 = so2 if so2 is not None else 0
    no2 = no2 if no2 is not None else 0
    co = co if co is not None else 0
    o3 = o3 if o3 is not None else 0
    
    pollutants = {'PM2.5': pm25, 'PM10': pm10, 'SO2': so2, 'NO2': no2, 'CO': co, 'O3': o3}
    max_pollutant = max(pollutants, key=pollutants.get)
    return max_pollutant
$$;

-- -----------------------------------------------------
-- 2. UDF: aqi_criteria_met
-- Purpose: Validates minimum pollutant measurements for AQI calculation
-- Business Rule: Requires 1 PM metric + 2 other pollutants
-- Returns: Count of criteria pollutants (0-3)
-- -----------------------------------------------------
CREATE OR REPLACE FUNCTION aqi_criteria_met(
    pm25 NUMBER, pm10 NUMBER, so2 NUMBER, no2 NUMBER, co NUMBER, o3 NUMBER
)
RETURNS NUMBER(38,0)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.12'
HANDLER = 'aqi_criteria_met'
COMMENT = 'Validates minimum pollutant measurements (1 PM + 2 others)'
AS $$
def aqi_criteria_met(pm25, pm10, so2, no2, co, o3):
    pm_count = 1 if (pm25 is not None and pm25 > 0) or (pm10 is not None and pm10 > 0) else 0
    non_pm_count = min(2, sum(p is not None and p > 0 for p in [so2, no2, co, o3]))
    return pm_count + non_pm_count
$$;

-- -----------------------------------------------------
-- 3. UDF: calculate_custom_aqi
-- Purpose: Calculates custom AQI as max pollutant if criteria met
-- Returns: Highest pollutant value or 0 if criteria not met
-- -----------------------------------------------------
CREATE OR REPLACE FUNCTION calculate_custom_aqi(
    pm25 NUMBER, pm10 NUMBER, so2 NUMBER, no2 NUMBER, co NUMBER, o3 NUMBER
)
RETURNS NUMBER(10,2)
LANGUAGE SQL
COMMENT = 'Returns highest pollutant value if criteria met, otherwise 0'
AS $$
    CASE
        WHEN aqi_criteria_met(pm25, pm10, so2, no2, co, o3) >= 3 
        THEN GREATEST(COALESCE(pm25, 0), COALESCE(pm10, 0), COALESCE(so2, 0), 
                     COALESCE(no2, 0), COALESCE(co, 0), COALESCE(o3, 0))
        ELSE 0
    END
$$;

-- =====================================================
-- 4. DYNAMIC TABLE: aqi_consumption_dt
-- Purpose: Analytical consumption layer with calculated metrics
-- Grain: One row per HOUR per location (averaged from multiple readings)
-- Features: Hourly aggregation, local time hierarchy, EPA/DEFRA categories, data quality flags, formatted location names
-- Multi-Country: Supports all countries with automatic timezone conversion
-- Location Formatting: Applies proper title case with Spanish article rules (e.g., San Juan de Lurigancho)
-- =====================================================

CREATE OR REPLACE DYNAMIC TABLE dev_db.consumption_sch.aqi_consumption_dt
    TARGET_LAG = 'DOWNSTREAM'
    WAREHOUSE = transform_wh
    COMMENT = 'Multi-country air quality consumption layer with hourly aggregation, local time hierarchy, and calculated metrics'
AS
WITH hourly_aggregated AS (
    -- Step 1: Aggregate multiple readings within same hour to single hourly average
    SELECT 
        -- Time grouping (truncate to hour in local timezone)
        DATE_TRUNC('HOUR', CONVERT_TIMEZONE(timezone_id, measurement_ts)) AS hour_timestamp_local,
        DATE_TRUNC('HOUR', measurement_ts) AS hour_timestamp_utc,
        
        -- Location grouping (formatted with proper title case)
        format_location_name(country) AS country,
        format_location_name(city) AS city,
        format_location_name(district) AS district,
        location_name, region, 
        latitude, longitude, timezone_id,
        
        -- Aggregated Air Quality Metrics (average of all readings in the hour)
        ROUND(AVG(pm2_5), 2) AS pm2_5,
        ROUND(AVG(pm10), 2) AS pm10,
        ROUND(AVG(so2), 2) AS so2,
        ROUND(AVG(no2), 2) AS no2,
        ROUND(AVG(co), 2) AS co,
        ROUND(AVG(o3), 2) AS o3,
        ROUND(AVG(us_epa_index), 0) AS us_epa_index,
        ROUND(AVG(gb_defra_index), 0) AS gb_defra_index,
        
        -- Aggregated Weather Context (average of all readings in the hour)
        ROUND(AVG(temp_c), 1) AS temp_c,
        ROUND(AVG(humidity), 0) AS humidity,
        ROUND(AVG(cloud_cover), 0) AS cloud_cover,
        ROUND(AVG(wind_kph), 1) AS wind_kph,
        MODE(wind_direction) AS wind_direction,  -- Most common wind direction
        ROUND(AVG(pressure_mb), 1) AS pressure_mb,
        ROUND(AVG(precip_mm), 2) AS precip_mm,
        ROUND(AVG(visibility_km), 1) AS visibility_km,
        ROUND(AVG(uv_index), 0) AS uv_index,
        MODE(weather_condition) AS weather_condition,  -- Most common condition
        
        -- Metadata (keep most recent file info)
        MAX(_stg_file_load_ts) AS _stg_file_load_ts,
        MAX(_copy_data_ts) AS _copy_data_ts,
        COUNT(*) AS readings_in_hour  -- Track how many readings were averaged
        
    FROM dev_db.clean_sch.clean_aqi_dt
    GROUP BY 
        DATE_TRUNC('HOUR', CONVERT_TIMEZONE(timezone_id, measurement_ts)),
        DATE_TRUNC('HOUR', measurement_ts),
        country, city, district, location_name, region,
        latitude, longitude, timezone_id
)
SELECT 
    -- Time Hierarchy (Local Time - primary for analysis)
    hour_timestamp_local AS aqi_timestamp,
    YEAR(hour_timestamp_local) AS aqi_year,
    MONTH(hour_timestamp_local) AS aqi_month,
    QUARTER(hour_timestamp_local) AS aqi_quarter,
    DAY(hour_timestamp_local) AS aqi_day,
    HOUR(hour_timestamp_local) AS aqi_hour,
    DAYOFWEEK(hour_timestamp_local) AS aqi_day_of_week,
    DAYNAME(hour_timestamp_local) AS aqi_day_name,
    
    -- UTC Reference (for audit/debugging)
    hour_timestamp_utc AS aqi_timestamp_utc,
    
    -- Location
    country, city, district, location_name, region, latitude, longitude, timezone_id,
    
    -- Air Quality Pollutants (hourly averages)
    pm2_5, pm10, so2, no2, co, o3, us_epa_index, gb_defra_index,
    
    -- Weather Context (hourly averages)
    temp_c, humidity, cloud_cover, wind_kph, wind_direction, pressure_mb, 
    precip_mm, visibility_km, uv_index, weather_condition,
    
    -- Calculated Metrics (applied AFTER hourly aggregation)
    prominent_pollutant(pm2_5, pm10, so2, no2, co, o3) AS prominent_pollutant,
    aqi_criteria_met(pm2_5, pm10, so2, no2, co, o3) AS criteria_pollutant_count,
    calculate_custom_aqi(pm2_5, pm10, so2, no2, co, o3) AS custom_aqi,
    
    -- Air Quality Classifications (based on hourly averages)
    CASE 
        WHEN us_epa_index = 1 THEN 'Good'
        WHEN us_epa_index = 2 THEN 'Moderate'
        WHEN us_epa_index = 3 THEN 'Unhealthy for Sensitive Groups'
        WHEN us_epa_index = 4 THEN 'Unhealthy'
        WHEN us_epa_index = 5 THEN 'Very Unhealthy'
        WHEN us_epa_index = 6 THEN 'Hazardous'
        ELSE 'Unknown'
    END AS epa_category,
    
    CASE 
        WHEN gb_defra_index BETWEEN 1 AND 3 THEN 'Low'
        WHEN gb_defra_index BETWEEN 4 AND 6 THEN 'Moderate'
        WHEN gb_defra_index BETWEEN 7 AND 9 THEN 'High'
        WHEN gb_defra_index = 10 THEN 'Very High'
        ELSE 'Unknown'
    END AS defra_category,
    
    -- Data Quality (based on hourly averages)
    CASE 
        WHEN pm2_5 IS NULL AND pm10 IS NULL THEN FALSE
        WHEN aqi_criteria_met(pm2_5, pm10, so2, no2, co, o3) >= 3 THEN TRUE
        ELSE FALSE
    END AS is_valid_measurement,
    
    -- Metadata
    readings_in_hour,  -- Number of raw readings averaged into this hour
    _stg_file_load_ts,
    _copy_data_ts
    
FROM hourly_aggregated;

-- =====================================================
-- ANALYTICAL VIEWS
-- Purpose: Pre-built queries for common access patterns
-- =====================================================

-- -----------------------------------------------------
-- 5. VIEW: vw_aqi_daily
-- Purpose: Daily aggregations by country, district, and location
-- Grain: One row per day per location (aggregated from hourly data)
-- Multi-Country: Aggregates all countries
-- -----------------------------------------------------
CREATE OR REPLACE VIEW dev_db.consumption_sch.vw_aqi_daily AS
SELECT 
    country, district, location_name,
    aqi_year, aqi_month, aqi_day, DATE(aqi_timestamp) AS measurement_date,
    ROUND(AVG(pm2_5), 2) AS daily_avg_pm2_5,
    ROUND(AVG(pm10), 2) AS daily_avg_pm10,
    ROUND(AVG(us_epa_index), 2) AS daily_avg_epa_index,
    MAX(us_epa_index) AS daily_max_epa_index,
    MODE(epa_category) AS predominant_category,
    ROUND(AVG(temp_c), 1) AS daily_avg_temp_c,
    COUNT(*) AS hourly_measurements,
    SUM(readings_in_hour) AS total_raw_readings  -- Total raw readings across all hours
FROM dev_db.consumption_sch.aqi_consumption_dt
GROUP BY country, district, location_name, aqi_year, aqi_month, aqi_day, DATE(aqi_timestamp);

-- -----------------------------------------------------
-- 6. VIEW: vw_aqi_current
-- Purpose: Latest hourly measurement per location (real-time status)
-- Grain: One row per country+district (most recent hourly average)
-- Multi-Country: Shows current hourly readings for all countries
-- -----------------------------------------------------
CREATE OR REPLACE VIEW dev_db.consumption_sch.vw_aqi_current AS
WITH latest_measurements AS (
    SELECT country, district, MAX(aqi_timestamp) AS latest_timestamp
    FROM dev_db.consumption_sch.aqi_consumption_dt
    GROUP BY country, district
)
SELECT 
    c.country, c.district, c.location_name, c.aqi_timestamp AS last_updated,
    c.latitude, c.longitude, c.timezone_id, c.pm2_5, c.pm10, c.us_epa_index, 
    c.epa_category, c.prominent_pollutant, c.temp_c, c.humidity,
    c.wind_kph, c.weather_condition, c.is_valid_measurement,
    c.readings_in_hour  -- Shows how many raw readings were averaged for this hour
FROM dev_db.consumption_sch.aqi_consumption_dt c
INNER JOIN latest_measurements lm
    ON c.country = lm.country AND c.district = lm.district AND c.aqi_timestamp = lm.latest_timestamp;

-- Script execution completed
SELECT 'Consumption layer objects created successfully' AS status;
