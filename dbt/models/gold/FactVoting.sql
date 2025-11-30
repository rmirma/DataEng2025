{{ config(
    materialized='incremental',
    unique_key='VotingId',
    incremental_strategy='append'
) }}

SELECT
	cityHash64(v.uuid) AS VotingId,
    v.uuid AS VotingSrcId,
    toInt32(cityHash64(v.type_code) % 2147483647) AS VotingType,
    cityHash64(v.uuid) AS WeatherId,
    toDate(v.start_date_time) AS Date,
    v.start_date_time AS StartTime,
    v.start_date_time AS EndTime,
    v.description AS VotingDescription,
    v.present AS Present,
    v.absent AS Absent,
    v.in_favor AS InFavour,
	v.against AS Against,
	v.neutral AS Neutral,
	v.abstained AS Abstained
FROM  {{ source('bronze_voting', 'votings') }} AS v

{% if is_incremental() %}
WHERE v.start_date_time > (SELECT max(StartTime) FROM {{ this }})
{% endif %}

GROUP BY
    v.type_code,
    v.uuid,
    v.start_date_time,
    v.start_date_time,
    v.description,
    v.present,
    v.absent,
    v.in_favor,
	v.against,
	v.neutral,
	v.abstained