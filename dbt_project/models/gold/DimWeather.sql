{{ config(
    materialized='incremental',
    unique_key='WeatherId',
    incremental_strategy='append'
) }}

SELECT
	v.WeatherId AS WeatherId,
	subtractHours(toStartOfHour(v.StartTime), 6) AS StartTime,
	toStartOfHour(v.StartTime) AS EndTime,
	AVG(w.temperature) AvgTemp,
	MAX(w.max_temperature) AS HighTemp,
	MIN(w.min_temperature) AS LowTemp,
	AVG(w.precipitation) AS AvgPrecipitation,
	MAX(w.precipitation) AS MaxPrecipitation,
	AVG(w.wind_speed) AvgWindSpeed,
	MAX(w.max_wind_speed) MaxWindSpeed
	
FROM {{ ref('voting') }} AS v
JOIN {{ ref('weather') }} AS w ON 
	subtractHours(toStartOfHour(v.StartTime), 6) <= toDateTime(concat(toString(w.date), ' ', w.time)) AND 
	toDateTime(concat(toString(w.date), ' ', w.time)) <= v.StartTime

{% if is_incremental() %}
WHERE v.start_date_time > (SELECT max(StartTime) FROM {{ ref('FactVoting')}})
{% endif %}

GROUP BY
    v.WeatherId, v.StartTime