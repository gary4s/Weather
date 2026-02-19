# Weather ETL Pipeline
A robust Data Engineering pipeline that extracts real-time weather data, transforms it for analytical use, and loads it into a Microsoft SQL Server database.

# Architecture
The pipeline follows a classic Extract, Transform, Load (ETL) pattern orchestrated by Apache Airflow:
    Extract: Fetch current weather data from WeatherAPI.com via REST API.
    Transform: Process raw JSON into a cleaned, structured DataFrame using Pandas, including timezone-aware UTC timestamps.
    Load: Append the data into a Microsoft SQL Server instance hosted on the local machine.

# Getting Started
    1. Prerequisites    Docker Desktop (with Compose enabled)
                        MS SQL Server (Local instance or Express)
                        WeatherAPI Key (Free tier)
    2. SQL Server ConfigurationTo allow Docker to communicate with your local SQL Server, ensure the following settings in SQL Server
    Configuration Manager:
    TCP/IP: Enabled
    Port: 1433 (Static)
    Authentication: Mixed Mode (SQL & Windows Auth enabled)
    Database: Create a database named weather.
    3. Environment VariablesCreate a .env file in your root directory:BashWEATHER_API_KEY=your_api_key_here

# Airflow Connection Setup
In the Airflow UI (Admin > Connections), create a connection named mssql_default:
Field   Value
Conn    Idmssql_default
Conn    Type Microsoft SQL Server
Host    [Your_Local_IP] (e.g., 192.168.1.50)
Schema  weatherLoginairflow_service_userPort1433

# Data Schema
The weather_reports table is structured as follows:
Column          Data Type   Description
city            VARCHAR     Name of the city
execution_date  DATETIME    Time of extraction (UTC)
temp            FLOAT       Temperature in Celsius
humidity        INT         Humidity percentage
conditions      VARCHAR     Visual description (e.g., "Cloudy")

# Key Features
Timezone Aware: Uses datetime.now(timezone.utc) to prevent "naive" timestamp bugs.
Fault Tolerant: Uses Airflow's retries logic to handle API timeouts or network blips.
Dockerized: The orchestration layer is fully containerized for easy deployment.

# Troubleshooting
Connection Error 20009: Ensure TCP/IP is enabled in SQL Server and the Windows Firewall allows port 1433.
extra TypeError: Ensure the "Extra" field in the Airflow Connection is completely empty.

