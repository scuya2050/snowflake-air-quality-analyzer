# SQL

This directory contains all SQL run against Snowflake, split by purpose.

---

## DDL — `/ddl`

Run these scripts in order. Each one depends on the objects created by the previous one. Execute them in a Snowflake worksheet with `ACCOUNTADMIN` (or trigger the `Deploy AQI SQL Objects` GitHub Actions workflow, which does this automatically).

| Script | What it creates |
|--------|----------------|
| `01-stage-layer.sql` | `raw_aqi` table, internal stage (`raw_stg`), JSON file format |
| `02-clean-layer.sql` | `clean_aqi_dt` dynamic table — deduplication and JSON flattening |
| `03-consumption-layer.sql` | Three Python UDFs + `aqi_consumption_dt` dynamic table (hourly aggregation) |
| `04-dimensional-model.sql` | Star schema: `date_dim`, `location_dim`, `air_quality_fact` |
| `05-streamlit-views.sql` | Six views used by the Streamlit dashboards + `GET_CURRENT_TIMEZONE` UDF |

### Dynamic table chain

The transformation layers are all dynamic tables that refresh automatically. The chain works like this:

```
raw_aqi  (stage table, loaded by copy task / ingestion script)
  └─ clean_aqi_dt           TARGET_LAG = DOWNSTREAM
       └─ aqi_consumption_dt  TARGET_LAG = DOWNSTREAM
            └─ date_dim        TARGET_LAG = 30 minutes
            └─ location_dim    TARGET_LAG = 30 minutes
            └─ air_quality_fact TARGET_LAG = 30 minutes
```

The `DOWNSTREAM` lag means the clean and consumption layers don't refresh on their own schedule — they only run when triggered by the 30-minute refresh of the fact table. This avoids unnecessary compute.

### UDFs (defined in `03-consumption-layer.sql`)

- **`format_location_name(name)`** — Converts `san_juan_de_lurigancho` → `San Juan de Lurigancho`. Handles Spanish articles (`de`, `del`, `la`, `el`, `los`, `las`, `y`, `e`, `a`, `al`) as lowercase when they're not the first word.
- **`prominent_pollutant(...)`** — Given all pollutant readings for a row, returns the name of the worst one based on relative thresholds.
- **`calculate_custom_aqi(...)`** — US EPA breakpoint-based AQI formula, applied at the consumption layer.

`format_location_name` is applied in `aqi_consumption_dt`, so its output flows into everything downstream without needing to apply it again.

### Streamlit views (defined in `05-streamlit-views.sql`)

These views act as the data contract between the transformation layer and the dashboards. Changing the underlying tables without updating these views shouldn't affect the dashboards.

| View | Used by | Grain |
|------|---------|-------|
| `vw_daily_city_agg` | Page 1 | Country / City / Day |
| `vw_hourly_city_agg` | Page 2 | Country / City / Hour |
| `vw_hourly_district_detail` | Page 3 | Country / City / District / Hour |
| `vw_latest_district_aqi` | Home page metrics | Latest reading per district |
| `vw_daily_district_aqi` | Page 4 | Country / City / District / Day |
| `vw_location_hierarchy` | Filter dropdowns (Pages 3, 4) | Distinct country/city/district combos |

---

## DML — `/dml`

| Script | What it does |
|--------|-------------|
| `01-copy-task.sql` | Defines the COPY INTO task that loads files from the internal stage into `raw_aqi`. The task is created suspended — resume it after confirming data loads correctly. |

---

## Tests — `/tests`

Basic validation queries to check that data loaded as expected at each layer. Not a formal test framework — just queries to run manually after a deployment.

| Script | What it checks |
|--------|---------------|
| `01-stage-tests.sql` | Row counts, null checks, and duplicate detection in `raw_aqi` |
| `02-clean-tests.sql` | Deduplication verification and data type checks in `clean_aqi_dt` |

---

## App — `/app`

| Script | What it does |
|--------|-------------|
| `deploy-streamlit-app.sql` | `CREATE OR REPLACE STREAMLIT` statement. Run by the `Deploy Streamlit App` GitHub Actions workflow after uploading page files to the stage. |
