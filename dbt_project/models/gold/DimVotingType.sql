{{ config(
    materialized='incremental',
    unique_key='VotingType',
    incremental_strategy='append'
) }}

SELECT
    toInt32(v.type_code) AS VotingType,
	v.type_value AS VotingTypeDesc
FROM {{ ref('voting') }} AS v

{% if is_incremental() %}
WHERE v.start_date_time > (SELECT max(StartTime) FROM {{ ref('FactVoting')}})
{% endif %}