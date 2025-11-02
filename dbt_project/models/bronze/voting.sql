{{ config(materialized='table') }}

SELECT     
    uuid,
    voting_number,
    type_code,
    type_value,
    description,
    start_date_time,
    end_date_time,
    present,
    absent,
    in_favor,
    against,
    neutral,
    abstained,
    sitting_title,
    sitting_date,
    created_at,
    updated_at
FROM parliament_data.votings