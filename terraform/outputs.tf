output "database_name" {
  description = "Name of the created database"
  value       = snowflake_database.main.name
}

output "schemas" {
  description = "Created schema names"
  value = {
    stage       = snowflake_schema.stage_sch.name
    clean       = snowflake_schema.clean_sch.name
    consumption = snowflake_schema.consumption_sch.name
    publish     = snowflake_schema.publish_sch.name
  }
}

output "warehouses" {
  description = "Created warehouse names"
  value = {
    load      = snowflake_warehouse.load_wh.name
    transform = snowflake_warehouse.transform_wh.name
    streamlit = snowflake_warehouse.streamlit_wh.name
    adhoc     = snowflake_warehouse.adhoc_wh.name
  }
}

output "stage_name" {
  description = "Name of the internal stage"
  value       = snowflake_stage.raw_stg.name
}

output "file_format_name" {
  description = "Name of the JSON file format"
  value       = snowflake_file_format.json_file_format.name
}