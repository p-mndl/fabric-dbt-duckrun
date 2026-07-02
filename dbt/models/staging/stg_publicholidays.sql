WITH source AS (
    SELECT * FROM {{ source('bronze', 'publicholidays') }}
)

SELECT *
FROM source
