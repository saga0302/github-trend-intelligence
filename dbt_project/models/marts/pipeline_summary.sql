-- models/marts/pipeline_summary.sql
-- High level summary of the entire pipeline
-- One row per hour showing total activity

WITH stars AS (
    SELECT hour, SUM(star_count) AS total_stars
    FROM {{ ref('stg_star_counts') }}
    GROUP BY hour
),

pushes AS (
    SELECT hour, SUM(push_count) AS total_pushes
    FROM {{ ref('stg_push_activity') }}
    GROUP BY hour
),

events AS (
    SELECT hour, SUM(event_count) AS total_events
    FROM {{ ref('stg_event_summary') }}
    GROUP BY hour
)

SELECT
    s.hour,
    s.total_stars,
    p.total_pushes,
    e.total_events,
    ROUND(s.total_stars * 100.0 / NULLIF(e.total_events, 0), 2) AS star_pct
FROM stars s
LEFT JOIN pushes p USING (hour)
LEFT JOIN events e USING (hour)
ORDER BY s.hour DESC