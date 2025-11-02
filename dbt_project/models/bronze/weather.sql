{{ config(materialized='table') }}
	
SELECT
id,
date,
temperature,
min_temperature,
max_temperature,
humidity,
wind_speed,
max_wind_speed,
precipitation,
time,
created_at
FROM weather_data.historic