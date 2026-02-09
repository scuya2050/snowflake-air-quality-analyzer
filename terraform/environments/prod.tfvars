environment   = "prod"
database_name = "PROD_DB"

warehouse_sizes = {
  load_wh      = "MEDIUM"
  transform_wh = "SMALL"
  streamlit_wh = "SMALL"
  adhoc_wh     = "X-SMALL"
}

tags = {
  project     = "air-quality-pipeline"
  environment = "prod"
  managed_by  = "terraform"
}
