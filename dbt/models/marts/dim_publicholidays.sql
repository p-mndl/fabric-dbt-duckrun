WITH staged AS (
    SELECT * FROM {{ ref('stg_publicholidays') }}
    where countryOrRegion = 'Germany'
)

SELECT
    countryOrRegion as Land,
    countryOrRegion,
    holidayName,
    normalizeHolidayName,
    IsPaidTimeOff,
    countryRegionCode,
    "date"
FROM staged
