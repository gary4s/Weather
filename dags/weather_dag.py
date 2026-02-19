from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook 
from datetime import datetime, timedelta, timezone
import requests
import pandas as pd
import os

# Configuration
API_KEY = os.getenv("WEATHER_API_KEY")
CITY = "London, UK"

def extract_transform_weather():
    # 1. EXTRACT
    url = f"http://api.weatherapi.com/v1/current.json?key={API_KEY}&q={CITY}&aqi=no"   

    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    
    # 2. TRANSFORM
    df = pd.DataFrame([{
        'city': data['location']['name'],
        'execution_date': datetime.now(timezone.utc),
        'temp': data['current']['temp_c'],       # Current temp in Celsius
        'humidity': data['current']['humidity'], # Humidity percentage
        'conditions': data['current']['condition']['text'] # e.g., "Partly cloudy"
    }])
    
    # 3. LOAD TO SQL SERVER (LENOVOGARY4)
    # Ensure 'mssql_default' is configured in Airflow Admin > Connections
    mssql_hook = MsSqlHook(mssql_conn_id='mssql_default')
    engine = mssql_hook.get_sqlalchemy_engine()
    
    # Using schema='dbo' is standard practice for SQL Server/SSMS
    df.to_sql('weather_reports', engine, if_exists='append', index=False, schema='dbo')
    print(f"Successfully loaded weather data for {CITY} into SQL Server database: weather")

# --- DAG Definition ---
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='weather_etl_v1',
    default_args=default_args,
    description='Fetches weather data and stores in SQL Server (SSMS)',
    schedule_interval='@hourly',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['weather', 'mssql', 'ssms']
) as dag:

    etl_task = PythonOperator(
        task_id='extract_transform_load',
        python_callable=extract_transform_weather
    )