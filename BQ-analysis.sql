-- View of weather categories
CREATE OR REPLACE VIEW WeatherCategories AS
WITH WeatherCategories AS (
    SELECT
        WeatherId,
        CASE
            WHEN MaxPercipitation = 0 THEN 'None'
            WHEN MaxPercipitation < 10 THEN 'Low'
            ELSE 'High'
        END AS PrecipitationCat,
        CASE
            WHEN AvgTemp < 0 THEN 'Low'
            WHEN AvgTemp BETWEEN 0 AND 20 THEN 'Medium'
            ELSE 'High'
        END AS TemperatureCat,
        CASE
            WHEN MaxWindSpeed < 10 THEN 'Low'
            WHEN MaxWindSpeed BETWEEN 10 AND 20 THEN 'Medium'
            ELSE 'High'
        END AS WindCat
    FROM DimWeather
)
SELECT * FROM WeatherCategories;

-- Attendance rate per sitting
SELECT
    f.VotingId,
    f.Date,
    ROUND(CAST(f.Present AS DECIMAL) / NULLIF((f.Present + f.Absent), 0) * 100, 2) AS AttendanceRate
FROM FactVoting f;

-- Consensus rate per vote
SELECT
    VotingId,
    ROUND(
      GREATEST(InFavour, Against, Neutral, Abstained)::DECIMAL /
      NULLIF((InFavour + Against + Neutral + Abstained), 0) * 100, 2
    ) AS ConsensusRate
FROM FactVoting;

-- Average attendance and consensus rates by weather categories
SELECT
    w.PrecipitationCat,
    ROUND(AVG(f.Present::DECIMAL / NULLIF((f.Present + f.Absent), 0)) * 100, 2) AS AvgAttendanceRate,
    ROUND(AVG(
      GREATEST(f.InFavour, f.Against, f.Neutral, f.Abstained)::DECIMAL /
      NULLIF((f.InFavour + f.Against + f.Neutral + f.Abstained), 0) * 100
    ), 2) AS AvgConsensusRate
FROM FactVoting f
JOIN WeatherCategories w ON f.WeatherID = w.WeatherId
GROUP BY w.PrecipitationCat
ORDER BY w.PrecipitationCat;

-- Average attendance and consensus rates by temperature categories
SELECT
    w.TemperatureCat,
    ROUND(AVG(f.Present::DECIMAL / NULLIF((f.Present + f.Absent), 0)) * 100, 2) AS AvgAttendanceRate,
    ROUND(AVG(
      GREATEST(f.InFavour, f.Against, f.Neutral, f.Abstained)::DECIMAL /
      NULLIF((f.InFavour + f.Against + f.Neutral + f.Abstained), 0) * 100
    ), 2) AS AvgConsensusRate
FROM FactVoting f
JOIN WeatherCategories w ON f.WeatherID = w.WeatherId
GROUP BY w.TemperatureCat
ORDER BY w.TemperatureCat;

-- Average attendance and consensus rates by wind categories
SELECT
    w.WindCat,
    ROUND(AVG(f.Present::DECIMAL / NULLIF((f.Present + f.Absent), 0)) * 100, 2) AS AvgAttendanceRate,
    ROUND(AVG(
      GREATEST(f.InFavour, f.Against, f.Neutral, f.Abstained)::DECIMAL /
      NULLIF((f.InFavour + f.Against + f.Neutral + f.Abstained), 0) * 100
    ), 2) AS AvgConsensusRate
FROM FactVoting f
JOIN WeatherCategories w ON f.WeatherID = w.WeatherId
GROUP BY w.WindCat
ORDER BY w.WindCat;

-- Average attendance and consensus rates by weekdays
SELECT
    d.Weekday,
    ROUND(AVG(f.Present::DECIMAL / NULLIF((f.Present + f.Absent), 0)) * 100, 2) AS AvgAttendanceRate,
    ROUND(AVG(
      GREATEST(f.InFavour, f.Against, f.Neutral, f.Abstained)::DECIMAL /
      NULLIF((f.InFavour + f.Against + f.Neutral + f.Abstained), 0) * 100
    ), 2) AS AvgConsensusRate
FROM FactVoting f
JOIN DimDate d ON f.Date = d.Date
GROUP BY d.Weekday
ORDER BY d.Weekday;
