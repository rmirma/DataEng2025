{{ config(
    materialized='incremental',
    unique_key='WeatherId',
    incremental_strategy='append'
) }}

-- depends_on: {{ ref('FactVoting') }}

SELECT
	cityHash64(v.uuid) AS WeatherId,
	subtractHours(toStartOfHour(v.start_date_time), 6) AS StartTime,
	toStartOfHour(v.start_date_time) AS EndTime,
	AVG(w.temperature) AvgTemp,
	MAX(w.max_temperature) AS HighTemp,
	MIN(w.min_temperature) AS LowTemp,
	AVG(w.precipitation) AS AvgPrecipitation,
	MAX(w.precipitation) AS MaxPrecipitation,
	AVG(w.wind_speed) AvgWindSpeed,
	MAX(w.max_wind_speed) MaxWindSpeed
	
FROM {{ source('bronze_voting', 'votings') }} AS v
JOIN {{ source('bronze_weather', 'historic') }} AS w ON 
	subtractHours(toStartOfHour(v.start_date_time), 6) <= toDateTime(concat(toString(w.date), ' ', w.time)) AND 
	toDateTime(concat(toString(w.date), ' ', w.time)) <= v.start_date_time

{% if is_incremental() %}
WHERE v.start_date_time > (SELECT max(StartTime) FROM {{ ref('FactVoting')}})
{% endif %}

GROUP BY
    cityHash64(v.uuid), v.start_date_time