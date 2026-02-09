-- =====================================================
-- Clean Layer Tests - Peru Air Quality
-- Purpose: Validate clean layer data quality and transformations
-- Target: dev_db.clean_sch.clean_peru_aqi_dt
-- =====================================================

USE ROLE accountadmin;
USE SCHEMA dev_db.clean_sch;
USE WAREHOUSE adhoc_wh;

-- =====================================================
-- TEST SUITE: CLEAN LAYER VALIDATION
-- =====================================================

-- Test 1: Table exists and has data
SELECT 
    'Test 1: Table Existence' AS test_name,
    CASE 
        WHEN COUNT(*) > 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS result,
    COUNT(*) AS record_count
FROM dev_db.clean_sch.clean_peru_aqi_dt;

-- Test 2: No duplicate measurements
SELECT 
    'Test 2: Duplicate Check' AS test_name,
    CASE 
        WHEN MAX(occurrence_count) = 1 THEN 'PASS'
        ELSE 'FAIL'
    END AS result,
    MAX(occurrence_count) AS max_duplicates,
    COUNT(*) AS potential_duplicates
FROM (
    SELECT 
        measurement_ts,
        location_name,
        latitude,
        longitude,
        COUNT(*) AS occurrence_count
    FROM dev_db.clean_sch.clean_peru_aqi_dt
    GROUP BY 1, 2, 3, 4
    HAVING COUNT(*) > 1
);

-- Test 3: All required fields populated
WITH field_completeness AS (
    SELECT 
        COUNT(*) AS total_records,
        COUNT(measurement_ts) AS has_measurement_ts,
        COUNT(location_name) AS has_location_name,
        COUNT(latitude) AS has_latitude,
        COUNT(longitude) AS has_longitude,
        COUNT(pm2_5) AS has_pm2_5,
        COUNT(pm10) AS has_pm10,
        COUNT(us_epa_index) AS has_us_epa_index,
        COUNT(_stg_file_name) AS has_file_name
    FROM dev_db.clean_sch.clean_peru_aqi_dt
)
SELECT 
    'Test 3: Required Fields' AS test_name,
    CASE 
        WHEN has_measurement_ts = total_records
         AND has_location_name = total_records
         AND has_latitude = total_records
         AND has_longitude = total_records
         AND has_file_name = total_records
        THEN 'PASS'
        ELSE 'FAIL'
    END AS result,
    total_records,
    has_measurement_ts,
    has_location_name,
    has_pm2_5,
    has_us_epa_index
FROM field_completeness;

-- Test 4: Data freshness (records within last 24 hours)
SELECT 
    'Test 4: Data Freshness' AS test_name,
    CASE 
        WHEN MAX(_copy_data_ts) >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
        THEN 'PASS'
        ELSE 'WARN'
    END AS result,
    MAX(_copy_data_ts) AS latest_load_time,
    DATEDIFF(hour, MAX(_copy_data_ts), CURRENT_TIMESTAMP()) AS hours_since_load
FROM dev_db.clean_sch.clean_peru_aqi_dt;

-- Test 5: District coverage
WITH district_check AS (
    SELECT 
        COUNT(DISTINCT district) AS districts_with_data
    FROM dev_db.clean_sch.clean_peru_aqi_dt
)
SELECT 
    'Test 5: District Coverage' AS test_name,
    CASE 
        WHEN districts_with_data >= 10 THEN 'PASS'
        ELSE 'WARN'
    END AS result,
    districts_with_data AS districts_found,
    'Expected: 10 Lima districts' AS description
FROM district_check;

-- Test 6: Valid numeric ranges
SELECT 
    'Test 6: Numeric Value Ranges' AS test_name,
    CASE 
        WHEN COUNT(*) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS result,
    COUNT(*) AS invalid_records
FROM dev_db.clean_sch.clean_peru_aqi_dt
WHERE pm2_5 < 0 OR pm2_5 > 500
   OR pm10 < 0 OR pm10 > 500
   OR temp_c < -50 OR temp_c > 60
   OR humidity < 0 OR humidity > 100
   OR us_epa_index < 1 OR us_epa_index > 6;

-- Test 7: Metadata integrity
SELECT 
    'Test 7: Metadata Integrity' AS test_name,
    CASE 
        WHEN COUNT(*) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS result,
    COUNT(*) AS records_missing_metadata
FROM dev_db.clean_sch.clean_peru_aqi_dt
WHERE _stg_file_name IS NULL
   OR _stg_file_load_ts IS NULL
   OR _stg_file_md5 IS NULL
   OR _copy_data_ts IS NULL;

-- Test 8: Timezone consistency
SELECT 
    'Test 8: Timezone Consistency' AS test_name,
    CASE 
        WHEN COUNT(DISTINCT timezone_id) = 1 
         AND MAX(timezone_id) = 'America/Lima'
        THEN 'PASS'
        ELSE 'WARN'
    END AS result,
    COUNT(DISTINCT timezone_id) AS unique_timezones,
    MAX(timezone_id) AS timezone
FROM dev_db.clean_sch.clean_peru_aqi_dt;

-- Test 9: Weather data completeness
SELECT 
    'Test 9: Weather Data Completeness' AS test_name,
    CASE 
        WHEN pct_with_weather >= 95.0 THEN 'PASS'
        WHEN pct_with_weather >= 80.0 THEN 'WARN'
        ELSE 'FAIL'
    END AS result,
    ROUND(pct_with_weather, 2) AS percent_with_weather
FROM (
    SELECT 
        (COUNT(temp_c) * 100.0 / COUNT(*)) AS pct_with_weather
    FROM dev_db.clean_sch.clean_peru_aqi_dt
);

-- Test 10: Deduplication effectiveness
WITH dedup_check AS (
    SELECT 
        COUNT(*) AS clean_count
    FROM dev_db.clean_sch.clean_peru_aqi_dt
),
raw_count AS (
    SELECT 
        COUNT(*) AS stage_count
    FROM dev_db.stage_sch.raw_aqi
    WHERE country = 'peru'
)
SELECT 
    'Test 10: Deduplication Logic' AS test_name,
    CASE 
        WHEN c.clean_count <= r.stage_count THEN 'PASS'
        ELSE 'FAIL'
    END AS result,
    r.stage_count AS raw_records,
    c.clean_count AS clean_records,
    (r.stage_count - c.clean_count) AS duplicates_removed
FROM clean_count c, raw_count r;

-- =====================================================
-- DATA QUALITY METRICS
-- =====================================================

SELECT 
    'CLEAN LAYER METRICS' AS report_section,
    COUNT(*) AS total_records,
    COUNT(DISTINCT measurement_ts) AS unique_timestamps,
    COUNT(DISTINCT district) AS unique_districts,
    MIN(measurement_ts) AS earliest_measurement,
    MAX(measurement_ts) AS latest_measurement,
    ROUND(AVG(pm2_5), 2) AS avg_pm2_5,
    ROUND(AVG(us_epa_index), 2) AS avg_epa_index,
    ROUND(AVG(temp_c), 1) AS avg_temperature
FROM dev_db.clean_sch.clean_peru_aqi_dt;

-- District-level summary
SELECT 
    'DISTRICT SUMMARY' AS report_section,
    district,
    COUNT(*) AS measurement_count,
    ROUND(AVG(pm2_5), 2) AS avg_pm2_5,
    ROUND(AVG(us_epa_index), 2) AS avg_epa_index,
    MIN(measurement_ts) AS first_measurement,
    MAX(measurement_ts) AS last_measurement
FROM dev_db.clean_sch.clean_peru_aqi_dt
GROUP BY district
ORDER BY district;

SELECT 'All clean layer tests completed' AS status;
