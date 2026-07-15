-- Deduplicates the raw-append bronze table written by dlt (NB_dlt_runner): every incremental
-- run appends changed issues as new row versions; only the latest version per id survives
-- here. This is the dlt/dbt boundary in one place — dlt lands data faithfully, everything
-- semantic starts in this model.

WITH source AS (
    SELECT * FROM {{ source('github', 'issues') }}
)

SELECT
    id,
    number,
    title,
    state,
    user__login AS author,
    created_at,
    updated_at
FROM source
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1
