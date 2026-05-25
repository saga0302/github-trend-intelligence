-- models/staging/stg_push_activity.sql

WITH source AS (
    SELECT
        repo_name,
        hour,
        push_count
    FROM {{ source('github_raw', 'push_activity') }}
    WHERE repo_name IS NOT NULL
      AND push_count > 0
),

renamed AS (
    SELECT
        repo_name,
        hour,
        push_count,
        DATE_TRUNC('day', hour) AS event_date,
        DATE_PART('hour', hour) AS event_hour
    FROM source
)

SELECT * FROM renamed