2024 Global Weather Analysis Dashboard
📌 Project Overview
This project serves as a comprehensive analysis of global temperature patterns using the NOAA GHCN (Global Historical Climatology Network) dataset. The objective was to architect a data pipeline—from raw data extraction in BigQuery to final visualization in Looker Studio—to identify regional climate anomalies and global averages for the year 2024.

🛠️ Tech Stack & Workflow
Data Source: Google BigQuery Public Datasets (processed over 100k+ rows of raw weather data).

Processing & ETL: Advanced SQL (Standard SQL) used for data cleaning, handling null values, and aggregating daily temperatures into monthly/regional metrics.

Visualization: Google Looker Studio (formerly Data Studio) used to build an interactive, stakeholder-ready dashboard.

📊 Key Insights
Global Baseline: The 2024 average temperature across all monitored global stations was recorded at 9.3°C.

Extreme Variance: Analysis detected a significant temperature range of 76.3°C between the highest recorded peak (38.0°C) and the lowest extreme (-38.3°C).

Geographic Concentration: Identified high-temperature clusters primarily localized in the Equatorial regions, specifically within Indonesia (ID) and American Samoa (AS).

🚀 Dashboard Features
KPI Scorecards: Real-time summary of global temperature metrics.

Heat Map Table: A ranked country-level analysis with conditional formatting for temperature intensity.

Geo-Spatial Mapping: A spatial distribution map to visualize temperature trends by latitude/longitude.

Time Series Analysis: Monthly trend lines to monitor seasonal fluctuations.
