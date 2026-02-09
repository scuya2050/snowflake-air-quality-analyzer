-- =====================================================
-- Stage Layer Tests - Peru Air Quality
-- Purpose: Validate stage layer objects and data quality
-- Target: dev_db.stage_sch.*
-- =====================================================

USE ROLE accountadmin;
USE SCHEMA dev_db.stage_sch;
USE WAREHOUSE adhoc_wh;

-- =====================================================
-- TEST SUITE: STAGE LAYER VALIDATION
-- =====================================================

-- Test 1: Stage exists and is accessible
SELECT 
    'Test 1: Stage Existence' AS test_name,
    CASE 
        WHEN COUNT(*) > 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS result,
    'Stage raw_stg exists' AS description
FROM INFORMATION_SCHEMA.STAGES
WHERE STAGE_SCHEMA = 'STAGE_SCH'
  AND STAGE_NAME = 'RAW_STG';

-- Test 2: File format exists
SELECT 
    'Test 2: File Format Existence' AS test_name,
    CASE 
        WHEN COUNT(*) > 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS result,
    'JSON file format exists' AS description
FROM INFORMATION_SCHEMA.FILE_FORMATS
WHERE FILE_FORMAT_SCHEMA = 'STAGE_SCH'
  AND FILE_FORMAT_NAME = 'JSON_FILE_FORMAT';

-- Test 3: Raw table exists
SELECT 
    'Test 3: Raw Table Existence' AS test_name,
    CASE 
        WHEN COUNT(*) > 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS result,
    'Table raw_aqi exists' AS description
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'STAGE_SCH'
  AND TABLE_NAME = 'RAW_AQI';

-- Test 4: Raw table has data
SELECT 
    'Test 4: Data Existence' AS test_name,
    CASE 
        WHEN COUNT(*) > 0 THEN 'PASS'
        ELSE 'WARN'
    END AS result,
    COUNT(*) AS record_count,
    'Raw table contains data' AS description
FROM dev_db.stage_sch.raw_aqi;

-- Test 5: Copy task exists
SELECT 
    'Test 5: Task Existence' AS test_name,
    CASE 
        WHEN COUNT(*) > 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS result,
    'Copy task exists' AS description
FROM INFORMATION_SCHEMA.TASKS
WHERE TASK_SCHEMA = 'STAGE_SCH'
  AND TASK_NAME = 'COPY_AIR_QUALITY_DATA';

-- Test 6: Files in stage
SELECT 
    'Test 6: Files in Stage' AS test_name,
    CASE 
        WHEN COUNT(*) > 0 THEN 'PASS'
        ELSE 'WARN'
    END AS result,
    COUNT(*) AS file_count,
    'Files available for processing' AS description
FROM (
    SELECT RELATIVE_PATH 
    FROM DIRECTORY(@raw_stg)
    WHERE RELATIVE_PATH LIKE '%country=peru%'
);

-- Test 7: Required metadata columns populated
SELECT 
    'Test 7: Metadata Completeness' AS test_name,
    CASE 
        WHEN missing_count = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS result,
    missing_count AS records_missing_metadata
FROM (
    SELECT COUNT(*) AS missing_count
    FROM dev_db.stage_sch.raw_aqi
    WHERE _stg_file_name IS NULL
       OR _stg_file_load_ts IS NULL
       OR _stg_file_md5 IS NULL
       OR _copy_data_ts IS NULL
);

-- Test 8: Valid JSON structure
SELECT 
    'Test 8: JSON Validity' AS test_name,
    CASE 
        WHEN invalid_count = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS result,
    invalid_count AS invalid_records
FROM (
    SELECT COUNT(*) AS invalid_count
    FROM dev_db.stage_sch.raw_aqi
    WHERE raw IS NULL
       OR TRY_PARSE_JSON(raw::STRING) IS NULL
);

-- Test 9: Peru data present
SELECT 
    'Test 9: Peru Data Filter' AS test_name,
    CASE 
        WHEN COUNT(*) > 0 THEN 'PASS'
        ELSE 'WARN'
    END AS result,
    COUNT(*) AS peru_records,
    'Peru-specific data exists' AS description
FROM dev_db.stage_sch.raw_aqi
WHERE country = 'peru';

-- Test 10: Partition columns populated
SELECT 
    'Test 10: Partition Attributes' AS test_name,
    CASE 
        WHEN missing_partitions = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS result,
    missing_partitions AS records_missing_partitions
FROM (
    SELECT COUNT(*) AS missing_partitions
    FROM dev_db.stage_sch.raw_aqi
    WHERE country IS NULL
       OR city IS NULL
       OR district IS NULL
       OR year IS NULL
       OR month IS NULL
       OR day IS NULL
);

-- =====================================================
-- DATA QUALITY METRICS
-- =====================================================

SELECT 
    'STAGE LAYER METRICS' AS report_section,
    COUNT(*) AS total_records,
    COUNT(DISTINCT country) AS unique_countries,
    COUNT(DISTINCT city) AS unique_cities,
    COUNT(DISTINCT district) AS unique_districts,
    COUNT(DISTINCT _stg_file_name) AS unique_files,
    MIN(_copy_data_ts) AS earliest_load,
    MAX(_copy_data_ts) AS latest_load,
    DATEDIFF(hour, MAX(_copy_data_ts), CURRENT_TIMESTAMP()) AS hours_since_last_load
FROM dev_db.stage_sch.raw_aqi;

-- Peru-specific metrics
SELECT 
    'PERU DATA BREAKDOWN' AS report_section,
    country,
    city,
    district,
    COUNT(*) AS record_count,
    MIN(_copy_data_ts) AS first_load,
    MAX(_copy_data_ts) AS last_load
FROM dev_db.stage_sch.raw_aqi
WHERE country = 'peru'
GROUP BY country, city, district
ORDER BY district;

SELECT 'All stage layer tests completed' AS status;
