-- =====================================================
-- Stage Layer DDL - Peru Air Quality
-- Purpose: Create stage layer objects for raw data landing
-- Dependencies: 
--   - dev_db.stage_sch schema (created via Terraform)
--   - adhoc_wh warehouse (created via Terraform)
--
-- Object Creation Order:
--   1. Internal Stage (raw_stg)
--   2. File Format (json_file_format)
--   3. Raw Landing Table (raw_aqi)
--
-- Version: 1.0.0
-- =====================================================

USE ROLE accountadmin;
USE SCHEMA dev_db.stage_sch;
USE WAREHOUSE adhoc_wh;

-- =====================================================
-- RAW LANDING TABLE
-- Purpose: Transient table for raw JSON storage with partition metadata
-- Grain: One row per file ingested
-- =====================================================

CREATE OR REPLACE TRANSIENT TABLE dev_db.stage_sch.raw_aqi (
    raw                      VARIANT,
    country                  VARCHAR,
    city                     VARCHAR,
    district                 VARCHAR,
    year                     NUMBER(4,0),
    month                    NUMBER(2,0),
    day                      NUMBER(2,0),
    _stg_file_name           VARCHAR,
    _stg_file_load_ts        TIMESTAMP_NTZ,
    _stg_file_md5            VARCHAR,
    _copy_data_ts            TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    _copy_data_user          VARCHAR DEFAULT CURRENT_USER(),
    _copy_data_role          VARCHAR DEFAULT CURRENT_ROLE()
)
COMMENT = 'Raw landing table for Peru air quality data - stores JSON payload with partition metadata';

-- Script execution completed
SELECT 'Stage layer objects created successfully' AS status;
