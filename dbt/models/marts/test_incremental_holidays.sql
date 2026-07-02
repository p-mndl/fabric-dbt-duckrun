{{ config(
    materialized='incremental',
    unique_key='holiday_id',
    incremental_strategy='merge'
) }}

WITH source AS (
    SELECT
        countryOrRegion || '-' || CAST(date AS VARCHAR) AS holiday_id,
        countryOrRegion,
        date,
        holidayName,
        '{{ var("run_label", "run1") }}' AS run_label
    FROM {{ ref('stg_publicholidays') }}
    WHERE countryOrRegion = '{{ var("country", "Germany") }}'
)

SELECT *
FROM source
