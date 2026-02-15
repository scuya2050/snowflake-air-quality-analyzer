-- =====================================================
-- Consumption Layer DDL - Peru Air Quality
-- Purpose: Create consumption layer with UDFs, dynamic table, and analytical views
-- Dependencies:
--   - dev_db.clean_sch.clean_peru_aqi_dt (from 02-clean-layer.sql)
--   - dev_db.consumption_sch schema (created via Terraform)
--   - transform_wh warehouse (created via Terraform)
--
-- Object Creation Order:
--   1. UDF: peru_prominent_pollutant
--   2. UDF: peru_aqi_criteria_met
--   3. UDF: peru_calculate_custom_aqi
--   4. Dynamic Table: peru_aqi_consumption_dt
--   5. View: vw_peru_aqi_daily
--   6. View: vw_peru_aqi_current
--
-- Version: 1.0.0
-- =====================================================

USE ROLE accountadmin;
USE SCHEMA dev_db.consumption_sch;
USE WAREHOUSE adhoc_wh;

-- =====================================================
-- USER-DEFINED FUNCTIONS (UDFs)
-- Purpose: Business logic functions for air quality calculations
-- =====================================================

-- -----------------------------------------------------
-- 1. UDF: peru_prominent_pollutant
-- Purpose: Identifies the pollutant with highest concentration
-- Returns: Pollutant name (PM2.5, PM10, SO2, NO2, CO, O3)
-- -----------------------------------------------------
CREATE OR REPLACE FUNCTION peru_prominent_pollutant(
    pm25 NUMBER, pm10 NUMBER, so2 NUMBER, no2 NUMBER, co NUMBER, o3 NUMBER
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.12'
HANDLER = 'peru_prominent_pollutant'
COMMENT = 'Returns the pollutant with the highest concentration value'
AS $$
def peru_prominent_pollutant(pm25, pm10, so2, no2, co, o3):
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
-- 2. UDF: peru_aqi_criteria_met
-- Purpose: Validates minimum pollutant measurements for AQI calculation
-- Business Rule: Requires 1 PM metric + 2 other pollutants
-- Returns: Count of criteria pollutants (0-3)
-- -----------------------------------------------------
CREATE OR REPLACE FUNCTION peru_aqi_criteria_met(
    pm25 NUMBER, pm10 NUMBER, so2 NUMBER, no2 NUMBER, co NUMBER, o3 NUMBER
)
RETURNS NUMBER(38,0)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.12'
HANDLER = 'peru_aqi_criteria_met'
COMMENT = 'Validates minimum pollutant measurements (1 PM + 2 others)'
AS $$
def peru_aqi_criteria_met(pm25, pm10, so2, no2, co, o3):
    pm_count = 1 if (pm25 is not None and pm25 > 0) or (pm10 is not None and pm10 > 0) else 0
    non_pm_count = min(2, sum(p is not None and p > 0 for p in [so2, no2, co, o3]))
    return pm_count + non_pm_count
$$;

-- -----------------------------------------------------
-- 3. UDF: peru_calculate_custom_aqi
-- Purpose: Calculates custom AQI as max pollutant if criteria met
-- Returns: Highest pollutant value or 0 if criteria not met
-- -----------------------------------------------------
CREATE OR REPLACE FUNCTION peru_calculate_custom_aqi(
    pm25 NUMBER, pm10 NUMBER, so2 NUMBER, no2 NUMBER, co NUMBER, o3 NUMBER
)
RETURNS NUMBER(10,2)
LANGUAGE SQL
COMMENT = 'Returns highest pollutant value if criteria met, otherwise 0'
AS $$
    CASE
        WHEN peru_aqi_criteria_met(pm25, pm10, so2, no2, co, o3) >= 3 
        THEN GREATEST(COALESCE(pm25, 0), COALESCE(pm10, 0), COALESCE(so2, 0), 
                     COALESCE(no2, 0), COALESCE(co, 0), COALESCE(o3, 0))
        ELSE 0
    END
$$;

-- =====================================================
-- 4. DYNAMIC TABLE: peru_aqi_consumption_dt
-- Purpose: Analytical consumption layer with calculated metrics
-- Grain: One row per measurement (hourly air quality readings)
-- Features: Time hierarchy, EPA/DEFRA categories, data quality flags
-- =====================================================

CREATE OR REPLACE DYNAMIC TABLE dev_db.consumption_sch.peru_aqi_consumption_dt
    TARGET_LAG = '30 min'
    WAREHOUSE = transform_wh
    COMMENT = 'Peru air quality consumption layer with time hierarchy, calculated metrics, and weather context'
AS
SELECT 
    -- Time Hierarchy
    measurement_ts AS aqi_timestamp,
    YEAR(measurement_ts) AS aqi_year,
    MONTH(measurement_ts) AS aqi_month,
    QUARTER(measurement_ts) AS aqi_quarter,
    DAY(measurement_ts) AS aqi_day,
    HOUR(measurement_ts) AS aqi_hour,
    DAYOFWEEK(measurement_ts) AS aqi_day_of_week,
    DAYNAME(measurement_ts) AS aqi_day_name,
    
    -- Location
    country, city, district, location_name, region, latitude, longitude, timezone_id,
    
    -- Air Quality Pollutants
    pm2_5, pm10, so2, no2, co, o3, us_epa_index, gb_defra_index,
    
    -- Weather Context
    temp_c, humidity, cloud_cover, wind_kph, wind_direction, pressure_mb, 
    precip_mm, visibility_km, uv_index, weather_condition,
    
    -- Calculated Metrics
    peru_prominent_pollutant(pm2_5, pm10, so2, no2, co, o3) AS prominent_pollutant,
    peru_aqi_criteria_met(pm2_5, pm10, so2, no2, co, o3) AS criteria_pollutant_count,
    peru_calculate_custom_aqi(pm2_5, pm10, so2, no2, co, o3) AS custom_aqi,
    
    -- Air Quality Classifications
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
    
    -- Data Quality
    CASE 
        WHEN pm2_5 IS NULL AND pm10 IS NULL THEN FALSE
        WHEN peru_aqi_criteria_met(pm2_5, pm10, so2, no2, co, o3) >= 3 THEN TRUE
        ELSE FALSE
    END AS is_valid_measurement,
    
    -- Metadata
    _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts
    
FROM dev_db.clean_sch.clean_peru_aqi_dt;

-- =====================================================
-- ANALYTICAL VIEWS
-- Purpose: Pre-built queries for common access patterns
-- =====================================================

-- -----------------------------------------------------
-- 5. VIEW: vw_peru_aqi_daily
-- Purpose: Daily aggregations by district and location
-- Grain: One row per day per location
-- -----------------------------------------------------
CREATE OR REPLACE VIEW dev_db.consumption_sch.vw_peru_aqi_daily AS
SELECT 
    aqi_year, aqi_month, aqi_day, DATE(aqi_timestamp) AS measurement_date,
    district, location_name,
    ROUND(AVG(pm2_5), 2) AS daily_avg_pm2_5,
    ROUND(AVG(pm10), 2) AS daily_avg_pm10,
    ROUND(AVG(us_epa_index), 2) AS daily_avg_epa_index,
    MAX(us_epa_index) AS daily_max_epa_index,
    MODE(epa_category) AS predominant_category,
    ROUND(AVG(temp_c), 1) AS daily_avg_temp_c,
    COUNT(*) AS hourly_measurements
FROM dev_db.consumption_sch.peru_aqi_consumption_dt
GROUP BY aqi_year, aqi_month, aqi_day, DATE(aqi_timestamp), district, location_name;

-- -----------------------------------------------------
-- 6. VIEW: vw_peru_aqi_current
-- Purpose: Latest measurement per district (real-time status)
-- Grain: One row per district (most recent reading)
-- -----------------------------------------------------
CREATE OR REPLACE VIEW dev_db.consumption_sch.vw_peru_aqi_current AS
WITH latest_measurements AS (
    SELECT district, MAX(aqi_timestamp) AS latest_timestamp
    FROM dev_db.consumption_sch.peru_aqi_consumption_dt
    GROUP BY district
)
SELECT 
    c.district, c.location_name, c.aqi_timestamp AS last_updated,
    c.latitude, c.longitude, c.pm2_5, c.pm10, c.us_epa_index, 
    c.epa_category, c.prominent_pollutant, c.temp_c, c.humidity,
    c.wind_kph, c.weather_condition, c.is_valid_measurement
FROM dev_db.consumption_sch.peru_aqi_consumption_dt c
INNER JOIN latest_measurements lm
    ON c.district = lm.district AND c.aqi_timestamp = lm.latest_timestamp;

-- Script execution completed
SELECT 'Consumption layer objects created successfully' AS status;
