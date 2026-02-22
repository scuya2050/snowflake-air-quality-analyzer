# Streamlit

The dashboard is deployed as a **native Streamlit app inside Snowflake** — it runs in Snowflake's infrastructure and connects directly to your data without any external server or credentials.

---

## Structure

```
streamlit_app.py        Entry point / navigation router
environment.yml         Python dependencies (Snowflake Anaconda channel)
pages/
    00-home.py          Overview: key metrics, data freshness, country breakdown
    01-air-quality-trend-city-day-level.py   Top 10 cities by AQI (latest date)
    02-air-quality-trend-city-hour-level.py  Hourly pollutant trends for a city
    03-air-quality-map.py                    Hourly district-level detail
    04-air-quality-map-bubble.py             Bubble map of districts in a city
utils/                  (reserved)
config.toml             Streamlit theme settings
```

---

## How navigation works

`streamlit_app.py` is the `MAIN_FILE` Snowflake executes. It only defines the navigation structure using `st.navigation()` and calls `pg.run()`. All page content lives in the `pages/` directory.

The sidebar labels come from the `title=` argument in each `st.Page()` call, not the filenames.

---

## Pages

**Home (`00-home.py`)**
Runs four queries on load to display: latest data date, city count, district count, and average current AQI. Also shows data freshness (time since last fact table write) and a country breakdown table.

**Page 1 — Daily City Trends**
Queries `vw_daily_city_agg` and shows the top 10 cities by AQI for the most recent date in the data. No filters — always shows the latest snapshot.

**Page 2 — Hourly City Trends**
Cascading selectors: Country → City → Date → Pollutant. Renders a line chart and bar chart of the selected pollutant across all hours in the day.

**Page 3 — District Detail**
Cascading selectors: Country → City → District → Date → Pollutant. Shows an AQI line chart for the selected district, then the selected pollutant's hourly trend.

**Page 4 — City Bubble Map**
Cascading selectors: Country → City → Date. Loads `vw_daily_district_aqi` and plots all districts on a `st.map()` bubble map, sized by AQI. Shows summary metrics (district count, average/max/min AQI) and a sortable data table below the map.

---

## environment.yml

Snowflake only allows packages from its own Anaconda channel. The file looks like:

```yaml
channels:
  - snowflake
dependencies:
  - python=3.11
  - streamlit
  - snowflake-snowpark-python
  - pandas
  - numpy
```

A few things to know:
- Only the `snowflake` channel is allowed — `conda-forge` and `defaults` will cause a deployment error.
- Version ranges like `>=1.30` are not supported. Either pin to an exact version or leave the version unpinned.
- This file **replaces** Snowflake's default environment, so you need to explicitly list `snowflake-snowpark-python` and `pandas` even though they'd normally be available by default.

---

## Deployment

The `Deploy Streamlit App` GitHub Actions workflow handles deployment. It:

1. Configures a Snowflake CLI connection using the `streamlit-deploy` connection name
2. Uploads `environment.yml` to the stage root
3. Uploads `streamlit_app.py` to the stage root
4. Uploads all `pages/*.py` files to the `pages/` subdirectory in the stage
5. Runs `sql/app/deploy-streamlit-app.sql` which issues the `CREATE OR REPLACE STREAMLIT` statement

The stage (`streamlit_stage`) is created by Terraform in `publish_sch`.

---

## Local development

Native Snowflake Streamlit apps can't be run locally in the same way — `get_active_session()` only works inside Snowflake. For local testing you'd need to replace the session with a manually created Snowpark session using your credentials.
