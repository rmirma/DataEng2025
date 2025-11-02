SELECT
	Date,
	WeekDay,
	HolidayInd,
	HolidayDesc	
FROM file('/data/2024-dates.csv')