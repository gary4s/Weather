from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests
import pandas as pd
import os
from dotenv import load_dotenv

# Configuration - Best practice: Use Environment Variables
API_KEY = os.getenv("WEATHER_API_KEY")
CITY = "London"

def extract_transform_weather():
    # 1. Extract
    # Using Visual Crossing as the example
    url = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{CITY}?unitGroup=metric&key={API_KEY}&contentType=json"
    
    response = requests.get(url)
    response.raise_for_status() # Best practice: Error handling
    data = response.json()
    
    # 2. Transform
    # Flattening the daily forecast
    current_data = data['days'][0]
    df = pd.DataFrame([{
        'city': data['address'],
        'execution_date': datetime.now(),
        'temp': current_data['temp'],
        'humidity': current_data['humidity'],
        'conditions': current_data['conditions']
    }])
    
    # 3. Load via Airflow PostgresHook
    # This uses the connection ID 'my_postgres_conn' we set up in the UI
    pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')
    engine = pg_hook.get_sqlalchemy_engine()
    
    df.to_sql('weather_reports', engine, if_exists='append', index=False)
    print(f"Successfully loaded weather data for {CITY}")

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
    description='Fetches weather data and stores in Postgres',
    schedule_interval='@hourly',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['weather', 'api']
) as dag:

    etl_task = PythonOperator(
        task_id='extract_transform_load',
        python_callable=extract_transform_weather
    )