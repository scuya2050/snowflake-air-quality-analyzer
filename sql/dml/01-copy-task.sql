-- =====================================================
-- Stage Layer DML - Copy Task
-- Purpose: Automated data ingestion from stage to raw table
-- Dependencies:
--   - dev_db.stage_sch.raw_aqi (from 01-stage-layer.sql)
--   - dev_db.stage_sch.raw_stg (from 01-stage-layer.sql)
--   - dev_db.stage_sch.json_file_format (from 01-stage-layer.sql)
--   - load_wh warehouse (created via Terraform)
--
-- Execution Order:
--   1. Create COPY task (suspended by default)
--   2. Grant task execution permissions
--
-- Note: Task remains SUSPENDED after creation
--       Run ALTER TASK...RESUME in operational-commands.sql to activate
-- Version: 1.0.0
-- =====================================================

USE ROLE accountadmin;
USE SCHEMA dev_db.stage_sch;
USE WAREHOUSE adhoc_wh;

-- =====================================================
-- 1. COPY TASK - AUTOMATED DATA INGESTION
-- Schedule: Hourly at :00 (CRON: 0 * * * * America/Lima)
-- Warehouse: load_wh
-- Error Handling: ABORT_STATEMENT on error
-- =====================================================

CREATE OR REPLACE TASK copy_air_quality_data
    WAREHOUSE = load_wh
    SCHEDULE = 'USING CRON 0 * * * * America/Lima'  -- Every hour at :00
    COMMENT = 'Automated task to copy Peru air quality data from stage to raw table'
AS
COPY INTO dev_db.stage_sch.raw_aqi (
    raw, country, city, district, year, month, day, 
    _stg_file_name, _stg_file_load_ts, _stg_file_md5
) 
FROM (
    SELECT 
        t.$1 AS raw,
        REGEXP_SUBSTR(METADATA$FILENAME, 'country=([^/]+)', 1, 1, 'e', 1) AS country,
        REGEXP_SUBSTR(METADATA$FILENAME, 'city=([^/]+)', 1, 1, 'e', 1) AS city,
        REGEXP_SUBSTR(METADATA$FILENAME, 'district=([^/]+)', 1, 1, 'e', 1) AS district,
        CAST(REGEXP_SUBSTR(METADATA$FILENAME, 'year=([0-9]{4})', 1, 1, 'e', 1) AS INT) AS year,
        CAST(REGEXP_SUBSTR(METADATA$FILENAME, 'month=([0-9]{1,2})', 1, 1, 'e', 1) AS INT) AS month,
        CAST(REGEXP_SUBSTR(METADATA$FILENAME, 'day=([0-9]{1,2})', 1, 1, 'e', 1) AS INT) AS day,
        METADATA$FILENAME AS _stg_file_name,
        METADATA$FILE_LAST_MODIFIED AS _stg_file_load_ts,
        METADATA$FILE_CONTENT_KEY AS _stg_file_md5
    FROM @dev_db.stage_sch.raw_stg
    (FILE_FORMAT => dev_db.stage_sch.json_file_format) t
)
FILE_FORMAT = (FORMAT_NAME = 'dev_db.stage_sch.json_file_format') 
ON_ERROR = ABORT_STATEMENT;

-- =====================================================
-- 2. GRANT PERMISSIONS
-- Purpose: Allow sysadmin to execute tasks
-- =====================================================

USE ROLE accountadmin;
GRANT EXECUTE TASK, EXECUTE MANAGED TASK ON ACCOUNT TO ROLE sysadmin;
USE ROLE sysadmin;

-- Script execution completed
SELECT 'Copy task created successfully (currently suspended)' AS status;
SELECT 'Run: ALTER TASK dev_db.stage_sch.copy_air_quality_data RESUME; to activate' AS next_step;
