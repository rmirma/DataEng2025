CREATE DATABASE IF NOT EXISTS views;


CREATE OR REPLACE VIEW views.VotingBase AS
SELECT
    VotingId,
    VotingSrcId,
    VotingType,
    WeatherId,
    Date,
    StartTime,
    EndTime,
    VotingDescription,
    Present,
    Absent,
    InFavour,
    Against,
    Neutral,
    Abstained
FROM gold.Voting;


CREATE OR REPLACE VIEW views.VotingAnalytics AS
SELECT
    v.VotingId,
    v.VotingSrcId,
    v.VotingType,
    vt.VotingTypeDesc,
    v.WeatherId,
    w.StartTime     AS WeatherStartTime,
    w.EndTime       AS WeatherEndTime,
    w.AvgTemp,
    w.HighTemp,
    w.LowTemp,
    w.AvgPrecipitation,
    w.MaxPrecipitation,
    w.AvgWindSpeed,
    w.MaxWindSpeed,
    v.Date,
    d.WeekDay,
    d.HolidayInd,
    d.HolidayDesc,
    d.Year,
    d.Month,
    d.Season,
    v.StartTime,
    v.EndTime,
    v.VotingDescription,
    v.Present,
    v.Absent,
    v.InFavour,
    v.Against,
    v.Neutral,
    v.Abstained
FROM gold.Voting AS v
LEFT JOIN gold.DimVotingType AS vt
    ON v.VotingType = vt.VotingType
LEFT JOIN gold.DimWeather AS w
    ON v.WeatherId = w.WeatherId
LEFT JOIN gold.DimDate AS d
    ON v.Date = d.Date;