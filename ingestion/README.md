# Ingestion

The ingestion script pulls air quality data from WeatherAPI.com and loads it into `dev_db.stage_sch.raw_aqi` using Snowpark.

---

## How it runs

GitHub Actions triggers it on a schedule (`35 * * * *` — 35 minutes past every hour). You can also trigger it manually from the `Weather API Hourly Ingestion` workflow.

For local development, it falls back to a `.env` file in the project root if environment variables aren't set.

---

## What the script does

For each district in the configured list, it:

1. Calls `GET /current.json` on WeatherAPI.com with `aqi=yes`
2. Wraps the raw JSON response in a row with partition metadata (country, city, district, year, month, day)
3. Inserts the row into `stage_sch.raw_aqi`

The raw JSON is stored as-is in the `raw VARIANT` column. Parsing and flattening happen downstream in the clean layer dynamic table.

The ingestion timestamp and file metadata columns (`_stg_file_load_ts`, `_stg_file_md5`, etc.) are used for deduplication — when the same measurement appears in multiple ingestion runs, the clean layer keeps only the most recently ingested copy.

---

## Configuration

The script reads from environment variables. When running locally, create a `.env` file in the project root:

```
API_TOKEN=your_weatherapi_key
API_URL=http://api.weatherapi.com/v1/current.json
SNOWFLAKE_ORGANIZATION_NAME=your_org
SNOWFLAKE_ACCOUNT_NAME=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
```

When running in GitHub Actions, these come from repository secrets and variables.

The Snowflake account identifier is constructed as `{ORGANIZATION_NAME}-{ACCOUNT_NAME}` — this matches Snowflake's account identifier format for Snowpark connections.

---

## Adding locations

The script currently has one location function: `get_lima_air_quality_data()`, which covers 43 districts across Lima, Peru.

To add a new city:

1. Add a new function following the same pattern — define the `country`, `city`, and `districts` list, call the API for each district, and collect the results.
2. Call the new function from `main()`.
3. The dynamic tables downstream are country-agnostic, so they'll pick up the new data automatically.

---

## Error handling

If an individual district API call fails, the script logs the error and continues with the next district rather than aborting the entire run. Partial loads are fine because the deduplication in the clean layer handles gaps — a missing hour for one district doesn't affect any other district.

---

## Dependencies

```
snowflake-snowpark-python[pandas]
requests
python-dotenv
```
