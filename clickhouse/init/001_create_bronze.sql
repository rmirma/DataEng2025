CREATE DATABASE IF NOT EXISTS weather_data; --bronze
CREATE DATABASE IF NOT EXISTS parliament_data; --bronze
CREATE DATABASE IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS weather_data.historic
(
    id               UInt64 DEFAULT cityHash64(date, coalesce(time, '')),
    date             Date        NOT NULL,
    temperature      Nullable(Decimal(5,2)),
    min_temperature  Nullable(Decimal(5,2)),
    max_temperature  Nullable(Decimal(5,2)),
    humidity         Nullable(Decimal(5,2)),
    wind_speed       Nullable(Decimal(5,2)),
    max_wind_speed   Nullable(Decimal(5,2)),
    precipitation    Nullable(Decimal(5,2)),
    time             Nullable(String),
    created_at       DateTime DEFAULT now()
    -- PG had UNIQUE(date, time). CH doesn't enforce UNIQUE; we ensure idempotency by deleting the window before insert.
)
ENGINE = ReplacingMergeTree(created_at)
PARTITION BY toYYYYMM(date)
ORDER BY (date, time)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE TABLE IF NOT EXISTS parliament_data.votings
(
    uuid             String,
    voting_number    Nullable(Int32),
    type_code        Nullable(String),
    type_value       Nullable(String),
    description      Nullable(String),
    start_date_time  Nullable(DateTime),
    end_date_time    Nullable(DateTime),
    present          Nullable(Int32),
    absent           Nullable(Int32),
    in_favor         Nullable(Int32),
    against          Nullable(Int32),
    neutral          Nullable(Int32),
    abstained        Nullable(Int32),
    sitting_title    Nullable(String),
    sitting_date     Nullable(Date),
    created_at       DateTime DEFAULT now(),
    updated_at       DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(sitting_date)
ORDER BY (sitting_date, uuid)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE TABLE IF NOT EXISTS bronze.date_raw
(
    Date            Date,
	WeekDay         String,
	HolidayInd      Bool,
	HolidayDesc	    String,
    updated_at      DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(Date)
ORDER BY (Date);



	