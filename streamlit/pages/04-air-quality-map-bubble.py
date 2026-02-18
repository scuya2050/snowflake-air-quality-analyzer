import streamlit as st
import pandas as pd
from decimal import Decimal
from snowflake.snowpark.context import get_active_session

# Page Title
st.title("Air Quality Map - City Districts")
st.write("Bubble map showing AQI across all districts in selected city and date")

# Get Session
session = get_active_session()

country_option, city_option, date_option = '', '', ''

# Get distinct countries
country_query = """
    SELECT DISTINCT country 
    FROM dev_db.publish_sch.vw_daily_district_aqi 
    ORDER BY 1
"""
country_list = session.sql(country_query)
country_option = st.selectbox('Select Country', country_list)

# Check the selection
if (country_option is not None and len(country_option) > 1):
    city_query = f"""
    SELECT DISTINCT city 
    FROM dev_db.publish_sch.vw_daily_district_aqi 
    WHERE country = '{country_option}' 
    ORDER BY 1
    """
    city_list = session.sql(city_query)
    city_option = st.selectbox('Select City', city_list)

if (city_option is not None and len(city_option) > 1):
    date_query = f"""
    SELECT DISTINCT measurement_date 
    FROM dev_db.publish_sch.vw_daily_district_aqi 
    WHERE country = '{country_option}' 
      AND city = '{city_option}'
    ORDER BY 1 DESC
    """
    date_list = session.sql(date_query)
    date_option = st.selectbox('Select Date', date_list)

if (date_option is not None):
    # sql statement - Query daily district AQI for selected city and date
    sql_stmt = f"""
    SELECT 
        district,
        location_name,
        latitude, 
        longitude,
        aqi,
        pm25_avg,
        pm10_avg,
        prominent_pollutant,
        hourly_readings_in_day
    FROM dev_db.publish_sch.vw_daily_district_aqi
    WHERE country = '{country_option}'
      AND city = '{city_option}'
      AND measurement_date = '{date_option}'
      AND latitude IS NOT NULL 
      AND longitude IS NOT NULL
    ORDER BY aqi DESC
    """

    # create a data frame
    sf_df = session.sql(sql_stmt).collect()

    if len(sf_df) > 0:
        pd_df = pd.DataFrame(
            sf_df,
            columns=['District', 'Location', 'lat', 'lon', 'AQI', 'PM2.5', 'PM10', 'Prominent Pollutant', 'Hourly Readings'])

        # Convert lat/lon to float for mapping
        columns_to_convert = ['lat', 'lon']
        pd_df[columns_to_convert] = pd_df[columns_to_convert].astype(float)
        
        # Display summary metrics
        st.markdown("---")
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Districts", len(pd_df))
        
        with col2:
            avg_aqi = pd_df['AQI'].mean()
            st.metric("Avg AQI", f"{avg_aqi:.0f}")
        
        with col3:
            max_aqi = pd_df['AQI'].max()
            worst_district = pd_df[pd_df['AQI'] == max_aqi]['District'].iloc[0]
            st.metric("Highest AQI", f"{max_aqi:.0f}", delta=worst_district)
        
        with col4:
            min_aqi = pd_df['AQI'].min()
            best_district = pd_df[pd_df['AQI'] == min_aqi]['District'].iloc[0]
            st.metric("Lowest AQI", f"{min_aqi:.0f}", delta=best_district)
        
        # Display bubble map
        st.markdown("---")
        st.subheader(f"{city_option}, {country_option} - {date_option}")
        st.map(pd_df[['lat', 'lon', 'AQI']], size='AQI')
        
        # Display data table
        st.markdown("---")
        st.subheader("District Details")
        st.dataframe(
            pd_df,
            column_config={
                "District": "District",
                "Location": "Location Name",
                "AQI": st.column_config.NumberColumn("AQI", format="%.0f"),
                "PM2.5": st.column_config.NumberColumn("PM2.5", format="%.2f"),
                "PM10": st.column_config.NumberColumn("PM10", format="%.2f"),
                "Prominent Pollutant": "Primary Pollutant",
                "Hourly Readings": st.column_config.NumberColumn("Data Points", format="%d")
            },
            hide_index=True,
            use_container_width=True
        )
    else:
        st.warning(f"⚠️ No data available for {city_option} on {date_option}")