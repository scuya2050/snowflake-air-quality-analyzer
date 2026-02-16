import streamlit as st
import pandas as pd
from decimal import Decimal
from snowflake.snowpark.context import get_active_session

# Page Title
st.title("Air Quality Map - Current Status")
st.write("Real-time map showing latest AQI across all districts")

# Get Session
session = get_active_session()

# sql statement - Query latest district AQI from publish schema
sql_stmt = """
SELECT 
    latitude, 
    longitude,
    aqi
FROM dev_db.publish_sch.vw_latest_district_aqi
WHERE latitude IS NOT NULL 
  AND longitude IS NOT NULL
"""

# create a data frame
sf_df = session.sql(sql_stmt).collect()

pd_df = pd.DataFrame(
        sf_df,
        columns=['lat','lon','AQI'])

columns_to_convert = ['lat', 'lon']
pd_df[columns_to_convert] = pd_df[columns_to_convert].astype(float)
st.map(pd_df, size='AQI')