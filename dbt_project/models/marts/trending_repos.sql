-- models/marts/trending_repos.sql
-- Identifies trending repos using z-score anomaly detection
-- A repo is trending when its star count is 2+ standard deviations above its average

WITH hourly_stars AS (
    SELECT * FROM {{ ref('stg_star_counts') }}
),

baseline AS (
    SELECT
        repo_name,
        AVG(star_count)    AS avg_hourly_stars,
        STDDEV(star_count) AS stddev_stars,
        COUNT(*)           AS hours_observed
    FROM hourly_stars
    GROUP BY repo_name
),

recent AS (
    SELECT
        repo_name,
        SUM(star_count)    AS recent_stars,
        MAX(hour)          AS latest_hour
    FROM hourly_stars
    GROUP BY repo_name
)

SELECT
    r.repo_name,
    r.recent_stars,
    r.latest_hour,
    ROUND(b.avg_hourly_stars, 2)                                           AS avg_hourly_stars,
    ROUND(b.stddev_stars, 2)                                               AS stddev_stars,
    ROUND((r.recent_stars - b.avg_hourly_stars)
          / NULLIF(b.stddev_stars, 0), 2)                                  AS z_score,
    b.hours_observed
FROM recent r
JOIN baseline b USING (repo_name)
WHERE b.hours_observed >= 1
  AND r.recent_stars >= 5
ORDER BY r.recent_stars DESC, z_score DESC NULLS LAST