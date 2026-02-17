# Import python packages
import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session

# Page Title
st.title("AQI Trend - By Country/City/Hour Level")
st.write("Hourly pollutant trends for selected city and date")

# Get Session
session = get_active_session()

# variables to hold the selection parameters, initiating as empty string
country_option, city_option, date_option = '','',''

# query to get distinct countries from vw_hourly_city_agg view
country_query = """
    SELECT DISTINCT country 
    FROM dev_db.publish_sch.vw_hourly_city_agg 
    ORDER BY 1
"""

# execute query using sql api and execute it by calling collect function.
country_list = session.sql(country_query)

# use the selectbox api to render the countries
country_option = st.selectbox('Select Country', country_list)

#check the selection
if (country_option is not None and len(country_option) > 1):

    # query to get distinct cities from vw_hourly_city_agg view
    city_query = f"""
    SELECT DISTINCT city 
    FROM dev_db.publish_sch.vw_hourly_city_agg 
    WHERE country = '{country_option}' 
    ORDER BY 1
    """
    # execute query using sql api and execute it by calling collect function.
    city_list = session.sql(city_query)

    # use the selectbox api to render the cities
    city_option = st.selectbox('Select City', city_list)

if (city_option is not None and len(city_option) > 1):
    date_query = f"""
        SELECT DISTINCT DATE(measurement_time) AS measurement_date 
        FROM dev_db.publish_sch.vw_hourly_city_agg 
        WHERE country = '{country_option}' 
          AND city = '{city_option}'
        ORDER BY 1 DESC
    """
    date_list = session.sql(date_query)
    date_option = st.selectbox('Select Date', date_list)

# Pollutant selector (only show when date is selected)
pollutant_option = None
if (date_option is not None):
    st.markdown("---")
    
    # Single select for pollutant
    pollutant_option = st.selectbox(
        '📊 Select Pollutant to Visualize:',
        options=['PM2.5', 'PM10', 'SO2', 'NO2', 'CO', 'O3'],
        index=0,  # Default to PM2.5
        help="Choose which pollutant to analyze over time"
    )

if (date_option is not None and pollutant_option is not None):
    trend_sql = f"""
    SELECT 
        aqi_hour AS Hour,
        pm25_avg,
        pm10_avg,
        so2_avg,
        no2_avg,
        co_avg,
        o3_avg
    FROM dev_db.publish_sch.vw_hourly_city_agg
    WHERE country = '{country_option}' 
      AND city = '{city_option}' 
      AND DATE(measurement_time) = '{date_option}'
    ORDER BY Hour
    """
    sf_df = session.sql(trend_sql).collect()

    # create panda's dataframe
    pd_df = pd.DataFrame(
        sf_df,
        columns=['Hour','PM2.5','PM10','SO2','NO2','CO','O3'])
    
    # Filter dataframe to only show selected pollutant
    pd_df_filtered = pd_df[['Hour', pollutant_option]]
    
    #draw charts
    st.subheader(f"📈 {pollutant_option} Hourly Trend")
    st.line_chart(pd_df_filtered, x='Hour', y=pollutant_option)
    st.divider()
    st.subheader(f"📊 {pollutant_option} Hourly Bar Chart")
    st.bar_chart(pd_df_filtered, x='Hour', y=pollutant_option)