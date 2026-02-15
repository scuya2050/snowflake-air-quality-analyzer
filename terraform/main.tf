terraform {
  required_version = ">= 1.5.0"

  required_providers {
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = "~> 0.90"
    }
  }

  # Use LOCAL backend for learning (simplest)
  backend "local" {
    path = "terraform.tfstate"
  }
}

provider "snowflake" {
  # Uses environment variables:
  # SNOWFLAKE_ORGANIZATION_NAME, SNOWFLAKE_ACCOUNT_NAME, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD
}

# ==============================================================================
# DATABASE
# ==============================================================================

resource "snowflake_database" "main" {
  name    = var.database_name
  comment = "Air quality data pipeline ${var.environment} database"
}

# ==============================================================================
# SCHEMAS
# ==============================================================================

resource "snowflake_schema" "stage_sch" {
  database = snowflake_database.main.name
  name     = "STAGE_SCH"
  comment  = "Stage layer for raw data ingestion"
}

resource "snowflake_schema" "clean_sch" {
  database = snowflake_database.main.name
  name     = "CLEAN_SCH"
  comment  = "Clean layer for transformed data"
}

resource "snowflake_schema" "consumption_sch" {
  database = snowflake_database.main.name
  name     = "CONSUMPTION_SCH"
  comment  = "Consumption layer for analytics"
}

resource "snowflake_schema" "publish_sch" {
  database = snowflake_database.main.name
  name     = "PUBLISH_SCH"
  comment  = "Publish layer for data sharing"
}

# ==============================================================================
# WAREHOUSES
# ==============================================================================

resource "snowflake_warehouse" "load_wh" {
  name                      = "LOAD_WH"
  comment                   = "Load warehouse for loading all the JSON files"
  warehouse_size            = var.warehouse_sizes.load_wh
  auto_resume               = true
  auto_suspend              = 60
  enable_query_acceleration = false
  warehouse_type            = "STANDARD"
  min_cluster_count         = 1
  max_cluster_count         = var.environment == "prod" ? 2 : 1
  scaling_policy            = "STANDARD"
  initially_suspended       = true
}

resource "snowflake_warehouse" "transform_wh" {
  name                      = "TRANSFORM_WH"
  comment                   = "ETL warehouse for all transformation activity"
  warehouse_size            = var.warehouse_sizes.transform_wh
  auto_resume               = true
  auto_suspend              = 60
  enable_query_acceleration = false
  warehouse_type            = "STANDARD"
  min_cluster_count         = 1
  max_cluster_count         = 1
  scaling_policy            = "STANDARD"
  initially_suspended       = true
}

resource "snowflake_warehouse" "streamlit_wh" {
  name                      = "STREAMLIT_WH"
  comment                   = "Streamlit virtual warehouse"
  warehouse_size            = var.warehouse_sizes.streamlit_wh
  auto_resume               = true
  auto_suspend              = 600 # 10 minutes for Streamlit apps
  enable_query_acceleration = false
  warehouse_type            = "STANDARD"
  min_cluster_count         = 1
  max_cluster_count         = 1
  scaling_policy            = "STANDARD"
  initially_suspended       = true
}

resource "snowflake_warehouse" "adhoc_wh" {
  name                      = "ADHOC_WH"
  comment                   = "Adhoc warehouse for development and exploration"
  warehouse_size            = var.warehouse_sizes.adhoc_wh
  auto_resume               = true
  auto_suspend              = 60
  enable_query_acceleration = false
  warehouse_type            = "STANDARD"
  min_cluster_count         = 1
  max_cluster_count         = 1
  scaling_policy            = "STANDARD"
  initially_suspended       = true
}

# ==============================================================================
# FILE FORMATS
# ==============================================================================

resource "snowflake_file_format" "json_file_format" {
  name        = "JSON_FILE_FORMAT"
  database    = snowflake_database.main.name
  schema      = snowflake_schema.stage_sch.name
  format_type = "JSON"
  compression = "AUTO"
  comment     = "JSON file format for air quality data"
}

# ==============================================================================
# STAGES
# ==============================================================================

resource "snowflake_stage" "raw_stg" {
  name      = "RAW_STG"
  database  = snowflake_database.main.name
  schema    = snowflake_schema.stage_sch.name
  comment   = "All air quality raw data will be stored in this internal stage"
  directory = "ENABLE = true"
}