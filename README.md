# GitHub Trend Intelligence Pipeline

> An end-to-end batch data pipeline that identifies viral GitHub repositories before they appear on GitHub Trending — using a full medallion architecture with automated orchestration, transformation, and a live dashboard.

🔴 **[Live Dashboard](https://app-trend-intelligence-4jxujvylb87awhqbzjebkz.streamlit.app/)**

---

## What It Does

Processes 150,000+ public GitHub events every hour — pushes, stars, forks, pull requests — and surfaces repositories with unusual star velocity using z-score anomaly detection. A repo trending here typically appears on GitHub Trending 6-12 hours later.

---

## Architecture
GitHub Archive (public, hourly files)
↓
Apache Airflow (orchestration, runs at :05 every hour)
↓
Databricks Bronze (raw Delta table, append-only, ACID)
↓
Databricks Silver (cleaned, typed, deduplicated Delta table)
↓
Databricks Gold (aggregated metrics: star velocity, push activity)
↓
Snowflake RAW schema (loaded via Snowflake Spark connector)
↓
dbt Core (6 tested, documented models with lineage graph)
↓
Streamlit Dashboard (live public URL, auto-refreshes every 5 min)

---

## Stack

| Layer | Tool |
|---|---|
| Orchestration | Apache Airflow (Docker, local) |
| Processing | Databricks Serverless + PySpark |
| Storage | Delta Lake (Bronze/Silver/Gold medallion) |
| Warehouse | Snowflake (Standard, XSMALL warehouse) |
| Transformation | dbt Core (6 models, 20 tests) |
| Dashboard | Streamlit Community Cloud |
| Auth | OAuth 2.0 service principal (Databricks) |
| Secrets | Unity Catalog volume (Databricks) |

---

## Pipeline Details

### Medallion Architecture
- **Bronze** — Raw JSON events exactly as received from GitHub Archive. Append-only. 150K+ rows per hour.
- **Silver** — Cleaned, typed, deduplicated. `created_at` cast to timestamp. Nested structs flattened. Duplicates removed on `event_id`.
- **Gold** — Three aggregation tables: `star_counts` (WatchEvents per repo per hour), `push_activity` (PushEvents per repo per hour), `event_summary` (all event types by hour).

### Trend Detection
Z-score anomaly detection identifies repos with statistically unusual star velocity:
z_score = (recent_stars - avg_hourly_stars) / stddev_stars
A z_score > 2 means a repo is receiving 2+ standard deviations more stars than its historical average — a genuine anomaly, not just a popular repo.

### Scheduling
- Airflow DAG: `5 * * * *` (5 minutes past every hour)
- The 5-minute offset ensures GitHub Archive has finished publishing the previous hour's file
- dbt runs at `:10` via Mac crontab — 5 minutes after Airflow completes export

---

## dbt Models
sources (RAW)
github_raw.star_counts
github_raw.push_activity
github_raw.event_summary
↓
staging (views)
stg_star_counts
stg_push_activity
stg_event_summary
↓
marts (tables)
trending_repos      ← z-score anomaly detection
language_activity   ← event patterns by hour of day
pipeline_summary    ← hourly overview
20 data quality tests across all layers (not_null, unique).

---

## Dashboard Pages

1. **Trending Repos** — Top repos by star count with bar chart and full table
2. **Event Activity** — GitHub events by hour of day, line chart + heatmap
3. **Pipeline Summary** — Hourly stars, pushes, and total events over time

---

## Key Engineering Decisions

**Why append-only Bronze?** Bronze is the immutable source of truth. If Silver or Gold logic changes, reprocess from Bronze without re-downloading data.

**Why Snowflake after Databricks?** Databricks is optimized for distributed Spark processing. Snowflake is optimized for concurrent SQL analytics and native BI tool connections.

**Why dbt?** SQL without dbt is untested and undocumented. dbt gives version-controlled, tested, documented transformations with an auto-generated lineage graph.

**Why 5-minute scheduling offset?** GitHub Archive takes ~5 minutes after the hour to publish the previous hour's file. Running at :05 guarantees the file exists.

---

## Author

**Sagarika Raju** — MS Analytics, USC 2026  
[LinkedIn](https://linkedin.com/in/sagarika-raju) | [GitHub](https://github.com/saga0302)
