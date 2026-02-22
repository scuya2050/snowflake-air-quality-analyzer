# Snowflake Air Quality Analytics

An end-to-end data pipeline that pulls hourly air quality readings from WeatherAPI.com, loads them into Snowflake, and serves them through a native Streamlit dashboard.

The pipeline currently covers Lima (Peru) across 43 districts, with sample data also available for Delhi, Singapore, and UK cities.

---

## How it works

**Ingestion** runs every hour via GitHub Actions. The Python script calls WeatherAPI.com for each district, packages the raw JSON response, and inserts it into `stage_sch.raw_aqi` using Snowpark.

**Transformation** is handled entirely by Snowflake dynamic tables. Once data lands in the stage table, a chain of dynamic tables processes it automatically:

```
stage_sch.raw_aqi
    â†’ clean_sch.clean_aqi_dt      (deduplication, JSON flattening)
    â†’ consumption_sch.aqi_consumption_dt  (hourly aggregation per location)
    â†’ publish_sch.date_dim         (time dimension)
    â†’ publish_sch.location_dim     (location dimension)
    â†’ publish_sch.air_quality_fact (fact table, 30-min lag)
    â†’ publish_sch.*views*          (data contracts for Streamlit)
```

**Dashboards** are deployed as a native Streamlit app inside Snowflake. There are four pages: daily city rankings, hourly trends by city, district-level detail, and a bubble map.

---

## Project structure

```
.github/workflows/
    weather-api-hourly-ingestion.yaml   # Runs every hour at :35
    deploy-pipeline.yml                 # Deploys all SQL objects (manual trigger)
    deploy-streamlit.yml                # Deploys Streamlit app files (manual trigger)
    deploy-snowflake-infraestructure.yaml  # Runs Terraform (manual trigger)

ingestion/
    ingest_weather_api_data.py          # Ingestion script

sql/
    ddl/
        01-stage-layer.sql              # raw_aqi table, stage, file format
        02-clean-layer.sql              # clean_aqi_dt dynamic table
        03-consumption-layer.sql        # UDFs + aqi_consumption_dt dynamic table
        04-dimensional-model.sql        # Star schema (date_dim, location_dim, air_quality_fact)
        05-streamlit-views.sql          # Views used by Streamlit + GET_CURRENT_TIMEZONE UDF
    dml/
        01-copy-task.sql                # COPY task for loading files from stage
    tests/
        01-stage-tests.sql
        02-clean-tests.sql
    app/
        deploy-streamlit-app.sql        # CREATE OR REPLACE STREAMLIT statement

streamlit/
    streamlit_app.py                    # Navigation router (entry point)
    environment.yml                     # Conda dependencies for Snowflake
    pages/
        00-home.py                      # Overview, metrics, data freshness
        01-air-quality-trend-city-day-level.py
        02-air-quality-trend-city-hour-level.py
        03-air-quality-map.py
        04-air-quality-map-bubble.py

terraform/
    main.tf                             # Databases, schemas, warehouses, stage, task
    variables.tf
    environments/
        dev.tfvars
        prod.tfvars

config/
    credentials.yaml.template
    dev.yaml
    prod.yaml

data/raw/samples/                       # Sample API responses for local testing
```

---

## Setup

### Prerequisites

- Snowflake account with ACCOUNTADMIN access
- WeatherAPI.com API key (free tier works)
- Terraform >= 1.5 (for infrastructure deployment)
- Python 3.8+

### 1. Infrastructure

Terraform creates all Snowflake objects: databases, schemas, warehouses, the internal stage, and the copy task.

```powershell
cd terraform
terraform init
terraform apply -var-file="environments/dev.tfvars"
```

You need to set these environment variables or add them to a `terraform.tfvars` file (which is gitignored):

```
SNOWFLAKE_ACCOUNT
SNOWFLAKE_USER
SNOWFLAKE_PASSWORD
```

### 2. SQL objects

The DDL scripts build the transformation layers in order. You can run them manually in a Snowflake worksheet, or trigger the `Deploy AQI SQL Objects` workflow in GitHub Actions with `environment = dev`.

The workflow also has a `resume_task` toggle â€” leave it off on first run until you confirm the data is loading correctly.

### 3. GitHub secrets

The ingestion workflow needs these set as GitHub repository secrets:

| Secret | Description |
|--------|-------------|
| `SNOWFLAKE_ORGANIZATION_NAME` | First part of your account identifier |
| `SNOWFLAKE_ACCOUNT_NAME` | Second part of your account identifier |
| `SNOWFLAKE_USER` | Snowflake username |
| `SNOWFLAKE_PASSWORD` | Snowflake password |
| `API_TOKEN` | WeatherAPI.com API key |

And this repository variable:

| Variable | Example |
|----------|---------|
| `API_URL` | `http://api.weatherapi.com/v1/current.json` |

### 4. Local development

Copy the credentials template and fill in your values:

```bash
cp config/credentials.yaml.template config/credentials.yaml
```

Create a `.env` file in the root (already in `.gitignore`):

```
API_TOKEN=your_key
API_URL=http://api.weatherapi.com/v1/current.json
SNOWFLAKE_ORGANIZATION_NAME=...
SNOWFLAKE_ACCOUNT_NAME=...
SNOWFLAKE_USER=...
SNOWFLAKE_PASSWORD=...
```

Run the ingestion script directly:

```bash
pip install "snowflake-snowpark-python[pandas]" requests python-dotenv
python ingestion/ingest_weather_api_data.py
```

### 5. Streamlit deployment

The `Deploy Streamlit App` workflow uploads all page files to the Snowflake internal stage and creates or replaces the Streamlit app. It needs a separate `streamlit-deploy` Snowflake CLI connection configured as a GitHub secret.

---

## Transformation details

**Clean layer** deduplicates the raw table using a window function â€” when multiple files contain a reading for the same timestamp and location, only the latest ingestion is kept.

**Consumption layer** includes three Python UDFs:
- `format_location_name` â€” converts `san_juan_de_lurigancho` to `San Juan de Lurigancho` (handles Spanish articles correctly)
- `prominent_pollutant` â€” returns the pollutant with the highest relative concentration
- `calculate_custom_aqi` â€” US EPA AQI calculation

The hourly aggregation dynamic table groups readings by location and hour, averaging the pollutant values across all data points within that window.

**Dimensional model** uses local timestamps (not UTC) so you can compare time-of-day patterns across countries in the same query.

---

## Adding a new country/city

The pipeline is generic â€” it processes whatever is in `raw_aqi` regardless of country. To add a new location:

1. Add a function in `ingest_weather_api_data.py` following the pattern of `get_lima_air_quality_data()`
2. Call it from `main()`
3. The dynamic tables pick it up automatically on the next refresh

---

## Notes

- Dynamic table lag is set to `DOWNSTREAM` in the clean and consumption layers, so refreshes are triggered by the fact table's 30-minute lag rather than running independently.
- The copy task is created by Terraform but starts suspended. Resume it manually once you've verified a manual ingestion run loaded data correctly.
- `credentials.yaml` and `.env` are both in `.gitignore`. Don't commit them.
