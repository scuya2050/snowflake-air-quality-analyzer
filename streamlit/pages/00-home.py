# =====================================================
# Home Page
# Purpose: Dashboard overview, key metrics, and data freshness
# =====================================================

import streamlit as st
from snowflake.snowpark.context import get_active_session

# Main title
st.title("🌍 Air Quality Analytics Dashboard")

# Introduction
st.markdown("""
## Welcome to the Multi-Country Air Quality Monitoring System

This dashboard provides **real-time air quality insights** across multiple countries and cities,
powered by hourly API data and a dimensional data model in Snowflake.

---

### 📊 Available Dashboards:

Navigate using the **sidebar** to explore different views:

#### 📈 **Daily City Trends** (Page 1)
- Compare **top 10 cities** by Air Quality Index (AQI)
- View daily aggregated pollutant levels
- Identify most polluted cities at a glance

#### ⏰ **Hourly City Trends** (Page 2)
- Analyze **intraday pollution patterns** by city
- Interactive filters: Country → City → Date → Pollutant
- Visualize hourly trend for selected pollutant

#### 📍 **District Detail** (Page 3)
- Deep dive into **specific district measurements**
- AQI trend + selected pollutant hourly charts
- Filters: Country → City → District → Date → Pollutant

#### 🗺️ **City Bubble Map** (Page 4)
- Geographic **bubble map** of AQI across all districts in a city
- Bubble size represents pollution intensity
- Filters: Country → City → Date

---

### 📋 Data Pipeline Architecture:

```
🔄 Hourly API Ingestion (GitHub Actions)
    ↓
📦 Stage Layer (Raw JSON)
    ↓
🧹 Clean Layer (Deduplication, 30 min lag)
    ↓
📊 Consumption Layer (Hourly aggregation)
    ↓
⭐ Dimensional Model (Star schema: Fact + Dimensions)
    ↓
📈 Streamlit Views (Optimized for visualization)
```

---

### 🌐 Current Data Coverage:
""")

# Get active session
session = get_active_session()

# Display key metrics in columns
col1, col2, col3, col4 = st.columns(4)

try:
    # Latest data date
    with col1:
        latest_date_query = """
            SELECT MAX(measurement_date)::VARCHAR AS latest_date
            FROM dev_db.publish_sch.vw_daily_city_agg
        """
        latest_date = session.sql(latest_date_query).collect()[0][0]
        st.metric("📅 Latest Data", latest_date)

    # Total cities monitored
    with col2:
        city_count_query = """
            SELECT COUNT(DISTINCT city) AS city_count
            FROM dev_db.publish_sch.vw_location_hierarchy
        """
        city_count = session.sql(city_count_query).collect()[0][0]
        st.metric("🏙️ Cities Monitored", city_count)

    # Total districts monitored
    with col3:
        district_count_query = """
            SELECT COUNT(DISTINCT district) AS district_count
            FROM dev_db.publish_sch.vw_location_hierarchy
        """
        district_count = session.sql(district_count_query).collect()[0][0]
        st.metric("📍 Districts Monitored", district_count)

    # Average current AQI
    with col4:
        avg_aqi_query = """
            SELECT ROUND(AVG(aqi), 0) AS avg_aqi
            FROM dev_db.publish_sch.vw_latest_district_aqi
        """
        avg_aqi = session.sql(avg_aqi_query).collect()[0][0]

        if avg_aqi <= 50:
            aqi_color = "🟢"
        elif avg_aqi <= 100:
            aqi_color = "🟡"
        elif avg_aqi <= 150:
            aqi_color = "🟠"
        else:
            aqi_color = "🔴"

        st.metric("🌡️ Avg Current AQI", f"{aqi_color} {int(avg_aqi)}")

except Exception as e:
    st.warning(f"⚠️ Unable to load metrics. Ensure data pipeline is running.")
    st.error(f"Error: {str(e)}")

# Data freshness indicator
st.markdown("---")
st.markdown("### 🔄 Data Freshness")

try:
    freshness_query = """
    SELECT 
        MAX(utc_timestamp)::TIMESTAMP_LTZ AS latest_reading,
        DATEDIFF('minute', MAX(utc_timestamp), CURRENT_TIMESTAMP()) AS minutes_old
    FROM dev_db.publish_sch.air_quality_fact
    """
    freshness_result = session.sql(freshness_query).collect()[0]
    latest_reading = freshness_result[0]
    minutes_old = freshness_result[1]

    col1, col2 = st.columns(2)
    with col1:
        st.info(f"**Latest Reading:** {latest_reading}")
    with col2:
        if minutes_old < 90:
            st.success(f"**Status:** ✅ Fresh ({minutes_old} minutes old)")
        else:
            st.warning(f"**Status:** ⚠️ Stale ({minutes_old} minutes old)")

except Exception as e:
    st.warning("Unable to check data freshness")
    st.error(f"Error: {str(e)}")

# Country breakdown
st.markdown("---")
st.markdown("### 🌎 Country Coverage")

try:
    country_query = """
    SELECT 
        country,
        COUNT(DISTINCT city) AS cities,
        COUNT(DISTINCT district) AS districts,
        ROUND(AVG(aqi), 0) AS avg_aqi
    FROM dev_db.publish_sch.vw_latest_district_aqi
    GROUP BY country
    ORDER BY country
    """
    country_df = session.sql(country_query).to_pandas()
    st.dataframe(
        country_df,
        column_config={
            "COUNTRY": "Country",
            "CITIES": st.column_config.NumberColumn("Cities", format="%d"),
            "DISTRICTS": st.column_config.NumberColumn("Districts", format="%d"),
            "AVG_AQI": st.column_config.NumberColumn("Avg AQI", format="%.0f")
        },
        hide_index=True,
        use_container_width=True
    )
except Exception as e:
    st.error(f"Unable to load country data: {str(e)}")

# Instructions
st.markdown("---")
st.markdown("""
### 🚀 Getting Started

1. **Select a dashboard** from the sidebar
2. **Use filters** to drill down into specific locations
3. **Interact with charts** - hover for details, zoom, pan
4. **Export data** using built-in Streamlit download options

---

### 📖 Understanding AQI Values:

| AQI Range | Category | Health Impact | Color |
|-----------|----------|---------------|-------|
| 0-50 | Good | Minimal impact | 🟢 Green |
| 51-100 | Moderate | Acceptable for most | 🟡 Yellow |
| 101-150 | Unhealthy for Sensitive Groups | May affect sensitive individuals | 🟠 Orange |
| 151-200 | Unhealthy | Everyone may experience effects | 🔴 Red |
| 201+ | Very Unhealthy / Hazardous | Serious health effects | 🟣 Purple |

---

### ⚙️ Technical Details:

- **Data Source:** WeatherAPI.com (Air Quality endpoint)
- **Update Frequency:** Hourly via GitHub Actions
- **Data Warehouse:** Snowflake (Dynamic Tables)
- **Refresh Lag:** 30 minutes target across all layers
- **Retention:** 30 days hot data (configurable)

---

👈 **Select a dashboard from the sidebar to begin!**
""")

# Footer
st.markdown("---")
st.caption(f"Air Quality Analytics Dashboard | Streamlit v{st.__version__} | Powered by Snowflake | Data updated hourly")
