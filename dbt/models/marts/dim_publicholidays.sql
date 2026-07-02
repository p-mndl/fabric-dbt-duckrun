WITH staged AS (
    SELECT * FROM {{ ref('stg_publicholidays') }}
    where countryOrRegion = 'Germany'
)

SELECT *
FROM staged
