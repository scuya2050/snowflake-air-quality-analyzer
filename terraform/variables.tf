variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be dev, staging, or prod."
  }
}

variable "database_name" {
  description = "Name of the Snowflake database"
  type        = string
  default     = "DEV_DB"
}

variable "warehouse_sizes" {
  description = "Warehouse sizes by type"
  type = object({
    load_wh      = string
    transform_wh = string
    streamlit_wh = string
    adhoc_wh     = string
  })
  default = {
    load_wh      = "MEDIUM"
    transform_wh = "X-SMALL"
    streamlit_wh = "X-SMALL"
    adhoc_wh     = "X-SMALL"
  }
}

variable "tags" {
  description = "Tags to apply to all resources"
  type        = map(string)
  default = {
    project    = "air-quality-pipeline"
    managed_by = "terraform"
  }
}
