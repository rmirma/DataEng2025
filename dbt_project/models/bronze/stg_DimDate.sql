SELECT
	Date,
	WeekDay,
	HolidayInd,
	HolidayDesc	
FROM file('/var/lib/clickhouse/user_files/2024-dates.csv')