CREATE DATABASE IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.DimDate
(
	Date Date,
	WeekDay String,
	HolidayInd Bool,
	HolidayDesc String,
	Year String,
	Month String,
    Season String
)
ENGINE = MergeTree
ORDER BY Date;

CREATE TABLE IF NOT EXISTS gold.DimVotingType
(
	VotingType Int32,
    VotingTypeDesc String
)
ENGINE = MergeTree
ORDER BY VotingType;


CREATE TABLE IF NOT EXISTS gold.DimWeather
(
	WeatherId UInt64,
	StartTime DateTime,
	EndTime Decimal(5,2),
	AvgTemp Decimal(5,2),
	HighTemp Decimal(5,2),
	LowTemp Decimal(5,2),
	AvgPrecipitation Decimal(5,2),
	MaxPrecipitation Decimal(5,2),
	AvgWindSpeed Decimal(5,2),
	MaxWindSpeed Decimal(5,2)
)
ENGINE = MergeTree
ORDER BY WeatherId;


CREATE TABLE IF NOT EXISTS gold.Voting
(
    VotingId UInt64,
    VotingSrcId String,
    VotingType Int32,
    WeatherId UInt64,
    Date Date,
    StartTime DateTime,
    EndTime DateTime,
    VotingDescription String,
    Present Int32,
    Absent Int32,
    InFavour Int32,
	Against Int32,
	Neutral Int32,
	Abstained Int32
)
ENGINE = MergeTree
ORDER BY VotingId;
