-- sql/init.sql

-- Create warehouse database and user
CREATE DATABASE weather_db;
CREATE USER warehouse WITH PASSWORD 'warehouse';
GRANT ALL PRIVILEGES ON DATABASE weather_db TO warehouse;

\connect weather_db;

-- Staging schema: raw data, no constraints
CREATE SCHEMA staging;
CREATE TABLE staging.weather_raw (
    id          SERIAL PRIMARY KEY,
    fetched_at  TIMESTAMP DEFAULT NOW(),
    city        VARCHAR(100),
    latitude    FLOAT,
    longitude   FLOAT,
    date        DATE,
    raw_data    JSONB
);

-- Core schema: clean, typed, deduplicated
CREATE SCHEMA core;
CREATE TABLE core.daily_weather (
    id                  SERIAL PRIMARY KEY,
    city                VARCHAR(100) NOT NULL,
    date                DATE NOT NULL,
    temp_max_c          FLOAT,
    temp_min_c          FLOAT,
    precipitation_mm    FLOAT,
    wind_speed_max_kmh  FLOAT,
    weather_code        INTEGER,
    loaded_at           TIMESTAMP DEFAULT NOW(),
    UNIQUE(city, date)
);

GRANT ALL ON SCHEMA staging TO warehouse;
GRANT ALL ON SCHEMA core TO warehouse;
GRANT ALL ON ALL TABLES IN SCHEMA staging TO warehouse;
GRANT ALL ON ALL TABLES IN SCHEMA core TO warehouse;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA staging TO warehouse;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA core TO warehouse;