-- models/staging/stg_event_summary.sql

WITH source AS (
    SELECT
        event_type,
        hour,
        event_count
    FROM {{ source('github_raw', 'event_summary') }}
    WHERE event_type IS NOT NULL
),

renamed AS (
    SELECT
        event_type,
        hour,
        event_count,
        DATE_TRUNC('day', hour) AS event_date,
        DATE_PART('hour', hour) AS event_hour
    FROM source
)

SELECT * FROM renamed