{{ config(
    materialized='incremental',
    unique_key='VotingType',
    incremental_strategy='append'
) }}

-- depends_on: {{ ref('FactVoting') }}

SELECT
    toInt32(cityHash64(v.type_code) % 2147483647) AS VotingType,
	v.type_value AS VotingTypeDesc
FROM {{ source('bronze_voting', 'votings') }} AS v

{% if is_incremental() %}
WHERE v.start_date_time > (SELECT max(StartTime) FROM {{ ref('FactVoting')}})
{% endif %}