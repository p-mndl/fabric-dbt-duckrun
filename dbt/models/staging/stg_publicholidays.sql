WITH source AS (
    SELECT * FROM {{ source('bronze', 'publicholidays') }}
)

SELECT
    countryOrRegion,
    holidayName,
    normalizeHolidayName,
    IsPaidTimeOff,
    countryRegionCode,
    "date"
FROM source
