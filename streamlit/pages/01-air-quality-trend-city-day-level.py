# Import python packages
import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session

# Page Title
st.title("Air Quality Trend - City+Day Level")
st.write("Top 10 cities by AQI for the most recent date")

# Get Session
session = get_active_session()

# sql statement - Query daily city aggregations from publish schema
sql_stmt = """
SELECT 
    country, 
    city, 
    pm25_avg,
    pm10_avg,
    so2_avg,
    no2_avg,
    co_avg,
    o3_avg,
    prominent_pollutant,
    aqi 
FROM 
    dev_db.publish_sch.vw_daily_city_agg
WHERE 
    measurement_date = (SELECT MAX(measurement_date) FROM dev_db.publish_sch.vw_daily_city_agg)
ORDER BY aqi DESC 
LIMIT 10;
"""

# create a data frame
sf_df = session.sql(sql_stmt).collect()

pd_df = pd.DataFrame(
        sf_df,
        columns=['Country','City','PM2.5','PM10','SO2','NO2','CO','O3','Primary Pollutant','AQI'])

st.dataframe(pd_df)