# Snowflake Air Quality Analytics - End-to-End Data Engineering Project

[![Data Engineering](https://img.shields.io/badge/Data-Engineering-blue)](https://github.com)
[![Snowflake](https://img.shields.io/badge/Snowflake-Cloud%20Data%20Platform-29B5E8)](https://www.snowflake.com/)
[![Python](https://img.shields.io/badge/Python-3.8%2B-3776AB?logo=python)](https://www.python.org/)
[![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-FF4B4B?logo=streamlit)](https://streamlit.io/)

## 📋 Project Overview

This is a comprehensive end-to-end data engineering project that demonstrates modern data warehousing and analytics using Snowflake. The project ingests air quality data from multiple sources, processes it through a multi-layered architecture, and presents insights through interactive Streamlit dashboards.

### Key Features

- 🌍 **Multi-Source Data Integration**: API-based ingestion from India, Singapore, and UK air quality datasets
- 🏗️ **Modern Data Architecture**: Implements staging, clean, and consumption layers
- 🔄 **Automated Pipelines**: GitHub Actions for scheduled data ingestion
- 📊 **Interactive Dashboards**: Streamlit-based visualizations for air quality trends
- ☁️ **Snowflake Features**: Dynamic tables, tasks, UDFs, and marketplace integration
- 🌡️ **Weather Integration**: Combines air quality with weather data from Snowflake Marketplace

## 🏛️ Architecture

The project follows a modern medallion architecture pattern:

```
┌─────────────────┐
│   Data Sources  │
│  (API/Files)    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Ingestion      │◄── GitHub Actions
│  (Snowpark)     │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Stage Layer    │  Raw JSON storage
│  (dev_db)       │  with metadata
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Clean Layer    │  Flattened &
│  (dev_db)       │  Deduplicated
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Consumption     │  Facts, Dimensions
│     Layer       │  & Aggregations
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Streamlit     │  Interactive
│   Dashboards    │  Visualizations
└─────────────────┘
```

## 📁 Project Structure

```
snowflake-e2e-project/
│
├── README.md                    # This file
├── .gitignore                   # Git ignore patterns
├── requirements.txt             # Python dependencies
│
├── .github/
│   └── workflows/
│       └── air_quality_hourly.yml  # Automated data ingestion pipeline
│
├── config/
│   ├── dev.yaml                 # Development environment config
│   ├── prod.yaml                # Production environment config
│   └── credentials.yaml.template  # Credentials template (DO NOT commit actual creds)
│
├── docs/
│   ├── architecture/            # Architecture diagrams
│   ├── setup-guide.md          # Detailed setup instructions
│   └── troubleshooting.md      # Common issues and solutions
│
├── sql/
│   ├── ddl/                    # Data Definition Language scripts
│   │   ├── 01-db-schema-wh-ddl.sql
│   │   ├── 02-stage-layer-ddl-dml.sql
│   │   └── 03-clean-layer-ddl-dml.sql
│   ├── dml/                    # Data Manipulation Language scripts
│   │   ├── 04-clean-transpose-table.sql
│   │   ├── 05-wide-table-consumption.sql
│   │   ├── 06-fact-and-dim.sql
│   │   ├── 07-aggregated-fact-table.sql
│   │   ├── 08-loading-additional-data.sql
│   │   └── 09-data-sharing-agg-fact.sql
│   └── functions/              # User-Defined Functions
│
├── python/
│   ├── src/
│   │   ├── ingestion/
│   │   │   └── ingest-api-data.py  # API data ingestion script
│   │   └── utils/
│   │       ├── __init__.py
│   │       ├── snowflake_connector.py  # Snowflake connection utilities
│   │       └── config_loader.py        # Configuration loader
│   └── tests/                  # Unit tests
│
├── streamlit/
│   ├── pages/                  # Streamlit dashboard pages
│   │   ├── 01-air-quality-trend-city-day-level.py
│   │   ├── 02-air-quality-trend-city-hour-level.py
│   │   ├── 03-air-quality-map.py
│   │   ├── 04-air-quality-map-bubble.py
│   │   └── 05-delhi-aqi.py
│   ├── utils/                  # Dashboard utilities
│   ├── requirements.txt        # Streamlit-specific dependencies
│   └── config.toml            # Streamlit configuration
│
├── data/
│   ├── raw/
│   │   └── samples/           # Sample data files (for testing)
│   └── processed/             # Processed data (local testing only)
│
├── scripts/
│   ├── setup/
│   │   └── initial_setup.sh   # Initial project setup script
│   ├── deployment/
│   │   └── deploy.py          # Deployment automation
│   └── maintenance/
│       └── cleanup.py         # Cleanup utilities
│
└── exercises/                 # Learning exercises and tutorials
```

## 🚀 Getting Started

### Prerequisites

- **Snowflake Account**: Sign up at [snowflake.com](https://signup.snowflake.com/)
- **Python 3.8+**: [Download Python](https://www.python.org/downloads/)
- **Git**: [Download Git](https://git-scm.com/downloads)
- **Air Quality API Key**: Register at your preferred air quality data provider

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd snowflake-e2e-project
   ```

2. **Set up Python virtual environment**
   ```bash
   python -m venv venv
   
   # On Windows
   .\venv\Scripts\activate
   
   # On macOS/Linux
   source venv/bin/activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure credentials**
   ```bash
   # Copy the template
   cp config/credentials.yaml.template config/credentials.yaml
   
   # Edit with your Snowflake credentials
   # ⚠️ NEVER commit this file to Git!
   ```

5. **Set up Snowflake objects**
   ```bash
   # Run the DDL scripts in order
   # Execute sql/ddl/*.sql in Snowflake worksheet
   ```

6. **Configure environment variables**
   ```bash
   # Windows PowerShell
   $env:SNOWFLAKE_ACCOUNT="your-account"
   $env:SNOWFLAKE_USER="your-username"
   $env:SNOWFLAKE_PASSWORD="your-password"
   
   # Linux/macOS
   export SNOWFLAKE_ACCOUNT="your-account"
   export SNOWFLAKE_USER="your-username"
   export SNOWFLAKE_PASSWORD="your-password"
   ```

### Quick Start

1. **Run data ingestion**
   ```bash
   python python/src/ingestion/ingest-api-data.py
   ```

2. **Launch Streamlit dashboard**
   ```bash
   cd streamlit
   streamlit run pages/01-air-quality-trend-city-day-level.py
   ```

## 📊 Data Pipeline

### 1. Ingestion Layer
- **Source**: Air quality APIs (India, Singapore, UK)
- **Frequency**: Hourly (via GitHub Actions)
- **Technology**: Snowpark Python
- **Output**: Raw JSON in Snowflake stage tables

### 2. Stage Layer (Bronze)
- **Schema**: `dev_db.stage_sch`
- **Format**: Semi-structured JSON with metadata
- **Purpose**: Landing zone for raw data

### 3. Clean Layer (Silver)
- **Schema**: `dev_db.clean_sch`
- **Transformations**:
  - JSON flattening
  - Deduplication
  - Data type standardization
  - Data quality checks

### 4. Consumption Layer (Gold)
- **Schema**: `dev_db.consumption_sch`
- **Objects**:
  - Fact tables (aggregated air quality metrics)
  - Dimension tables (location, time)
  - Wide tables for analytics
  - Dynamic tables for real-time updates

### 5. Visualization Layer
- **Technology**: Streamlit
- **Features**:
  - City-level daily/hourly trends
  - Interactive maps with AQI markers
  - Bubble maps for multi-metric analysis
  - Delhi-specific deep dives

## 🔧 Technologies Used

| Category | Technology | Purpose |
|----------|-----------|---------|
| **Data Warehouse** | Snowflake | Cloud data platform |
| **Data Processing** | Snowpark Python | Data transformation |
| **Orchestration** | GitHub Actions | Automated workflows |
| **Visualization** | Streamlit | Interactive dashboards |
| **Language** | Python 3.8+ | Scripting and automation |
| **Version Control** | Git | Source code management |

## 🌟 Snowflake Features Demonstrated

- ✅ **Dynamic Tables**: Auto-refreshing materialized views
- ✅ **Tasks**: Scheduled job execution
- ✅ **Streams**: Change Data Capture (CDC)
- ✅ **Stages**: External and internal data staging
- ✅ **File Formats**: JSON parsing and schema inference
- ✅ **Marketplace**: Integration with Weather data
- ✅ **UDFs**: Custom SQL functions
- ✅ **Resource Monitors**: Cost control
- ✅ **Role-Based Access Control (RBAC)**

## 📈 Sample Dashboards

The project includes 5 interactive Streamlit dashboards:

1. **City-Day Trends**: Daily air quality patterns across cities
2. **City-Hour Trends**: Hourly granularity for detailed analysis
3. **Map View**: Geographical distribution of AQI
4. **Bubble Map**: Multi-dimensional pollutant visualization
5. **Delhi Deep Dive**: Focused analysis on Delhi air quality

## 🔐 Security Best Practices

- ❌ **NEVER** commit credentials to Git
- ✅ Use environment variables for sensitive data
- ✅ Keep `config/credentials.yaml` in `.gitignore`
- ✅ Use Snowflake service accounts for automation
- ✅ Implement row-level security where needed
- ✅ Regularly rotate passwords and API keys

## 📚 Learning Resources

- [Snowflake Documentation](https://docs.snowflake.com/)
- [Snowpark Python Guide](https://docs.snowflake.com/en/developer-guide/snowpark/python/index.html)
- [Streamlit Documentation](https://docs.streamlit.io/)
- [Air Quality API Documentation](https://aqicn.org/api/)

## 🤝 Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📝 License

This project is for educational purposes. Please ensure compliance with data provider terms of service.

## 🐛 Troubleshooting

See [docs/troubleshooting.md](docs/troubleshooting.md) for common issues and solutions.

## 📞 Contact

For questions or feedback, please open an issue in the repository.

---

**Built with ❄️ Snowflake and 🐍 Python**
