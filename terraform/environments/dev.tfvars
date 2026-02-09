environment   = "dev"
database_name = "DEV_DB"

warehouse_sizes = {
  load_wh      = "X-SMALL"
  transform_wh = "X-SMALL"
  streamlit_wh = "X-SMALL"
  adhoc_wh     = "X-SMALL"
}

tags = {
  project     = "air-quality-pipeline"
  environment = "dev"
  managed_by  = "terraform"
}
