# =====================================================
# Air Quality Analytics - App Router
# Purpose: Navigation controller for multi-page Streamlit app
# Deployment: Snowflake native Streamlit (Projects → Streamlit)
# =====================================================

import streamlit as st

pg = st.navigation(
    {
        "Overview": [
            st.Page("pages/00-home.py", title="Home", default=True),
        ],
        "Dashboards": [
            st.Page("pages/01-air-quality-trend-city-day-level.py", title="Daily City Trends"),
            st.Page("pages/02-air-quality-trend-city-hour-level.py", title="Hourly City Trends"),
            st.Page("pages/03-air-quality-map.py", title="District Detail"),
            st.Page("pages/04-air-quality-map-bubble.py", title="City Bubble Map"),
        ],
    }
)
pg.run()