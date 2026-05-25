-- models/marts/language_activity.sql
-- Event count by type and hour of day
-- Shows which hours of the day are most active per event type

WITH event_data AS (
    SELECT * FROM {{ ref('stg_event_summary') }}
)

SELECT
    event_type,
    event_hour,
    SUM(event_count)                    AS total_events,
    ROUND(AVG(event_count), 0)          AS avg_events_per_hour,
    MAX(event_count)                    AS peak_events,
    COUNT(DISTINCT event_date)          AS days_observed
FROM event_data
GROUP BY event_type, event_hour
ORDER BY event_type, event_hour