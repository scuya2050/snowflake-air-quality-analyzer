-- =====================================================
-- Streamlit Application Deployment
-- Purpose: Create/update Streamlit app object
-- Prerequisites:
--   - Views deployed: vw_daily_city_agg, vw_hourly_city_agg, vw_hourly_district_detail, vw_latest_district_aqi, vw_location_hierarchy
--   - Files uploaded to @dev_db.publish_sch.streamlit_stage (streamlit_app.py + pages/)
--   - Warehouse: adhoc_wh exists
-- Deployment: Part of app pipeline (deploy-streamlit.yml)
-- =====================================================

USE ROLE accountadmin;
USE WAREHOUSE adhoc_wh;
USE DATABASE dev_db;
USE SCHEMA publish_sch;

-- ==============================================================================
-- STREAMLIT APPLICATION
-- ==============================================================================

CREATE OR REPLACE STREAMLIT dev_db.publish_sch.air_quality_analytics
  ROOT_LOCATION = '@dev_db.publish_sch.streamlit_stage'
  MAIN_FILE = '/streamlit_app.py'
  QUERY_WAREHOUSE = adhoc_wh
  TITLE = 'Air Quality Analytics - Multi-Country Monitoring'
  COMMENT = 'Real-time air quality dashboard with hourly updates';

-- Script execution completed
SELECT 'Streamlit app deployed: air_quality_analytics' AS status;
