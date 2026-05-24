# dags/weather_dag.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests
import json
import logging

# Cities to track — easy to extend
CITIES = [
    {"name": "Phoenix",      "lat": 33.45,  "lon": -112.07},
    {"name": "New York",     "lat": 40.71,  "lon": -74.01},
    {"name": "London",       "lat": 51.51,  "lon": -0.13},
    {"name": "Tokyo",        "lat": 35.69,  "lon": 139.69},
]

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# ── Task 1: Extract ──────────────────────────────────────────────────────────

def extract_weather(**context):
    """Fetch raw weather data from Open Meteo and push to XCom."""
    execution_date = context["ds"]  # YYYY-MM-DD string
    results = []

    for city in CITIES:
        url = (
            "https://api.open-meteo.com/v1/forecast"
            f"?latitude={city['lat']}&longitude={city['lon']}"
            "&daily=temperature_2m_max,temperature_2m_min,"
            "precipitation_sum,wind_speed_10m_max,weather_code"
            "&timezone=auto"
            f"&start_date={execution_date}&end_date={execution_date}"
        )
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        results.append({
            "city":      city["name"],
            "latitude":  city["lat"],
            "longitude": city["lon"],
            "date":      execution_date,
            "raw":       response.json(),
        })
        logging.info(f"Extracted weather for {city['name']} on {execution_date}")

    # Push raw data to XCom so the next task can consume it
    context["ti"].xcom_push(key="raw_weather", value=results)


# ── Task 2: Transform ────────────────────────────────────────────────────────

def transform_weather(**context):
    """Parse raw JSON into clean records and stage them in Postgres."""
    raw_data = context["ti"].xcom_pull(key="raw_weather", task_ids="extract")
    hook = PostgresHook(postgres_conn_id="weather_postgres")

    staged_ids = []

    for record in raw_data:
        # Insert raw JSON into staging table
        insert_sql = """
            INSERT INTO staging.weather_raw (city, latitude, longitude, date, raw_data)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id;
        """
        result = hook.get_first(insert_sql, parameters=(
            record["city"],
            record["latitude"],
            record["longitude"],
            record["date"],
            json.dumps(record["raw"]),
        ))
        staged_ids.append(result[0])
        logging.info(f"Staged {record['city']} with staging id {result[0]}")

    context["ti"].xcom_push(key="staged_ids", value=staged_ids)


# ── Task 3: Load ─────────────────────────────────────────────────────────────

def load_weather(**context):
    """Transform staged raw JSON into clean core.daily_weather rows."""
    hook = PostgresHook(postgres_conn_id="weather_postgres")
    staged_ids = context["ti"].xcom_pull(key="staged_ids", task_ids="transform")

    for staging_id in staged_ids:
        # Pull the raw record back from staging
        row = hook.get_first(
            "SELECT city, date, raw_data FROM staging.weather_raw WHERE id = %s",
            parameters=(staging_id,)
        )
        city, date, raw_data = row
        daily = raw_data.get("daily", {})

        # Extract first (and only) day's values
        def first(key):
            vals = daily.get(key, [None])
            return vals[0] if vals else None

        upsert_sql = """
            INSERT INTO core.daily_weather
                (city, date, temp_max_c, temp_min_c,
                 precipitation_mm, wind_speed_max_kmh, weather_code)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (city, date)
            DO UPDATE SET
                temp_max_c         = EXCLUDED.temp_max_c,
                temp_min_c         = EXCLUDED.temp_min_c,
                precipitation_mm   = EXCLUDED.precipitation_mm,
                wind_speed_max_kmh = EXCLUDED.wind_speed_max_kmh,
                weather_code       = EXCLUDED.weather_code,
                loaded_at          = NOW();
        """
        hook.run(upsert_sql, parameters=(
            city,
            date,
            first("temperature_2m_max"),
            first("temperature_2m_min"),
            first("precipitation_sum"),
            first("wind_speed_10m_max"),
            first("weather_code"),
        ))
        logging.info(f"Loaded {city} / {date} into core.daily_weather")


# ── DAG definition ────────────────────────────────────────────────────────────

with DAG(
    dag_id="weather_etl",
    default_args=default_args,
    description="Daily batch ETL: Open Meteo API → Postgres warehouse",
    schedule_interval="0 6 * * *",   # runs every day at 6am
    start_date=datetime(2026, 5, 24),
    catchup=False,
    tags=["etl", "weather", "portfolio"],
) as dag:

    extract = PythonOperator(
        task_id="extract",
        python_callable=extract_weather,
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_weather,
    )

    load = PythonOperator(
        task_id="load",
        python_callable=load_weather,
    )

    extract >> transform >> load