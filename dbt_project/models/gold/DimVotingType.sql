{{ config(
    materialized='incremental',
    unique_key='VotingType',
    incremental_strategy='append'
) }}

-- depends_on: {{ ref('FactVoting') }}

SELECT
    toInt32(v.type_code) AS VotingType,
	v.type_value AS VotingTypeDesc
FROM {{ source('parliament_data', 'voting') }} AS v

{% if is_incremental() %}
WHERE v.start_date_time > (SELECT max(StartTime) FROM {{ ref('FactVoting')}})
{% endif %}