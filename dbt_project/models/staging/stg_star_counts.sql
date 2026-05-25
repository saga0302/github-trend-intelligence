-- models/staging/stg_star_counts.sql
-- Staging model: light cleaning of raw star counts from Snowflake RAW schema
-- Reads from the RAW tables loaded by Databricks

WITH source AS (
    SELECT
        repo_name,
        hour,
        star_count
    FROM {{ source('github_raw', 'star_counts') }}
    WHERE repo_name IS NOT NULL
      AND star_count > 0
),

renamed AS (
    SELECT
        repo_name,
        hour,
        star_count,
        DATE_TRUNC('day', hour)  AS event_date,
        DATE_PART('hour', hour)  AS event_hour
    FROM source
)

SELECT * FROM renamed