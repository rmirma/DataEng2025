{{ config(
    materialized='incremental',
    unique_key='Date',
    incremental_strategy='append'
) }}

SELECT
	toDate(d.Date) Date,
	d.WeekDay AS WeekDay,
	d.HolidayInd AS HolidayInd,
	d.HolidayDesc AS HolidayDesc,
	toYear(toDate(d.Date)) AS Year,
	toMonth(toDate(d.Date)) AS Month,
    CASE
        WHEN toMonth(toDate(d.Date)) IN (3,4,5) THEN 'Spring'
        WHEN toMonth(toDate(d.Date)) IN (6,7,8) THEN 'Summer'
		WHEN toMonth(toDate(d.Date)) IN (9,10,11) THEN 'Autumn'
		WHEN toMonth(toDate(d.Date)) IN (12,1,2) THEN 'Winter'
        ELSE NULL
    END AS Season
FROM {{ source('bronze', 'weather_raw') }} AS d
