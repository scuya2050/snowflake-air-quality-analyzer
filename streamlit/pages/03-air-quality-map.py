# Import python packages
import streamlit as st
import pandas as pd
from decimal import Decimal
from snowflake.snowpark.context import get_active_session

# Page Title
st.title("Air Quality Trend - At District Level")
st.write("Detailed hourly trends for selected district")

# Get Session
session = get_active_session()

country_option, city_option, district_option, date_option = '','','',''

# Get distinct countries
country_query = """
    SELECT DISTINCT country 
    FROM dev_db.publish_sch.vw_location_hierarchy 
    ORDER BY 1
"""
country_list = session.sql(country_query)
country_option = st.selectbox('Select Country', country_list)

# Check the selection
if (country_option is not None and len(country_option) > 1):
    city_query = f"""
    SELECT DISTINCT city 
    FROM dev_db.publish_sch.vw_location_hierarchy 
    WHERE country = '{country_option}' 
    ORDER BY 1
    """
    city_list = session.sql(city_query)
    city_option = st.selectbox('Select City', city_list)

if (city_option is not None and len(city_option) > 1):
    district_query = f"""
    SELECT DISTINCT district 
    FROM dev_db.publish_sch.vw_location_hierarchy 
    WHERE country = '{country_option}' 
      AND city = '{city_option}'
    ORDER BY 1
    """
    district_list = session.sql(district_query)
    district_option = st.selectbox('Select District', district_list)

if (district_option is not None and len(district_option) > 1):
    date_query = f"""
    SELECT DISTINCT DATE(measurement_time) AS measurement_date 
    FROM dev_db.publish_sch.vw_hourly_district_detail
    WHERE country = '{country_option}' 
      AND city = '{city_option}' 
      AND district = '{district_option}'
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
        country,
        city,
        district,
        latitude,
        longitude,
        pm25_avg,
        pm10_avg,
        so2_avg,
        no2_avg,
        co_avg,
        o3_avg,
        prominent_pollutant,
        aqi
    FROM dev_db.publish_sch.vw_hourly_district_detail
    WHERE country = '{country_option}' 
      AND city = '{city_option}' 
      AND district = '{district_option}'
      AND DATE(measurement_time) = '{date_option}'
    ORDER BY Hour
    """
    sf_df = session.sql(trend_sql).collect()

    df = pd.DataFrame(sf_df, columns=['Hour','country','city','district','lat','lon','PM2.5','PM10','SO2','NO2','CO','O3','PROMINENT_POLLUTANT','AQI'])
    
    # Create separate dataframes for different visualizations
    df_aqi = df[['Hour', 'AQI']]
    df_pollutant = df[['Hour', pollutant_option]]
    df_map = df[['lat', 'lon']].drop_duplicates()

    # AQI Trend Chart
    st.subheader(f"📈 Hourly AQI Level")
    st.line_chart(df_aqi, x="Hour", y="AQI", color='#FFA500')
    
    st.divider()
    
    # Selected Pollutant Charts
    st.subheader(f"📊 {pollutant_option} Hourly Trend")
    st.line_chart(df_pollutant, x="Hour", y=pollutant_option, color='#1f77b4')
    
    st.subheader(f"📊 {pollutant_option} Bar Chart")
    st.bar_chart(df_pollutant, x="Hour", y=pollutant_option)