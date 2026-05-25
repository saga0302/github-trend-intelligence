import streamlit as st
import snowflake.connector
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

# ============================================
# GITHUB TREND INTELLIGENCE DASHBOARD
# Reads from Snowflake ANALYTICS schema
# ============================================

st.set_page_config(
    page_title="GitHub Trend Intelligence",
    page_icon="🔥",
    layout="wide"
)

# ── Snowflake connection ──────────────────────
@st.cache_resource
def get_connection():
    return snowflake.connector.connect(
        user="SAGA",
        password=st.secrets["snowflake_password"],
        account="lglipcy-sy52934",
        warehouse="COMPUTE_WH",
        database="github_analytics",
        schema="ANALYTICS",
        role="ACCOUNTADMIN"
    )

@st.cache_data(ttl=300)
def run_query(query):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(query)
    columns = [desc[0] for desc in cur.description]
    data = cur.fetchall()
    return pd.DataFrame(data, columns=columns)

# ── Sidebar ───────────────────────────────────
st.sidebar.title("GitHub Trend Intelligence")
st.sidebar.markdown("Real-time GitHub activity analytics powered by a full medallion architecture pipeline.")
st.sidebar.markdown("---")
st.sidebar.markdown("**Stack**")
st.sidebar.markdown("Airflow · Databricks · Delta Lake · Snowflake · dbt")
st.sidebar.markdown("---")

page = st.sidebar.radio(
    "Navigate",
    ["🔥 Trending Repos", "📊 Event Activity", "🔧 Pipeline Summary"]
)

# ── Page 1: Trending Repos ────────────────────
if page == "🔥 Trending Repos":
    st.title("🔥 Trending GitHub Repositories")
    st.markdown("Repositories with unusual star velocity detected by z-score anomaly detection.")

    df = run_query("""
        SELECT
            REPO_NAME,
            RECENT_STARS,
            AVG_HOURLY_STARS,
            STDDEV_STARS,
            Z_SCORE,
            HOURS_OBSERVED,
            LATEST_HOUR
        FROM github_analytics.ANALYTICS.TRENDING_REPOS
        ORDER BY RECENT_STARS DESC
        LIMIT 30
    """)

    if df.empty:
        st.warning("No trending repos found. Run the pipeline to load data.")
    else:
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Repos Tracked", len(df))
        col2.metric("Top Repo Stars", int(df['RECENT_STARS'].max()))
        col3.metric("Avg Stars/Repo", round(df['RECENT_STARS'].mean(), 1))

        st.markdown("### Top Trending Repos")
        fig = px.bar(
            df.head(15),
            x='RECENT_STARS',
            y='REPO_NAME',
            orientation='h',
            color='RECENT_STARS',
            color_continuous_scale='Blues',
            title='Top 15 Repos by Star Count'
        )
        fig.update_layout(yaxis={'categoryorder': 'total ascending'}, height=500)
        st.plotly_chart(fig, use_container_width=True)

        st.markdown("### Full Trending Table")
        st.dataframe(
            df[['REPO_NAME', 'RECENT_STARS', 'AVG_HOURLY_STARS', 'Z_SCORE', 'LATEST_HOUR']],
            use_container_width=True
        )

# ── Page 2: Event Activity ────────────────────
elif page == "📊 Event Activity":
    st.title("📊 GitHub Event Activity by Hour")
    st.markdown("Shows which hours of the day are most active per event type (UTC).")

    df = run_query("""
        SELECT EVENT_TYPE, EVENT_HOUR, TOTAL_EVENTS, AVG_EVENTS_PER_HOUR
        FROM github_analytics.ANALYTICS.LANGUAGE_ACTIVITY
        ORDER BY EVENT_TYPE, EVENT_HOUR
    """)

    if df.empty:
        st.warning("No event data found.")
    else:
        event_types = df['EVENT_TYPE'].unique().tolist()
        selected = st.multiselect(
            "Select event types",
            event_types,
            default=['PushEvent', 'WatchEvent', 'ForkEvent', 'PullRequestEvent']
        )

        filtered = df[df['EVENT_TYPE'].isin(selected)]

        fig = px.line(
            filtered,
            x='EVENT_HOUR',
            y='TOTAL_EVENTS',
            color='EVENT_TYPE',
            title='GitHub Events by Hour of Day (UTC)',
            labels={'EVENT_HOUR': 'Hour (UTC)', 'TOTAL_EVENTS': 'Total Events'}
        )
        fig.update_layout(height=450)
        st.plotly_chart(fig, use_container_width=True)

        st.markdown("### Heatmap")
        pivot = filtered.pivot_table(
            index='EVENT_TYPE',
            columns='EVENT_HOUR',
            values='TOTAL_EVENTS',
            fill_value=0
        )
        fig2 = px.imshow(
            pivot,
            title='Event Heatmap by Type and Hour',
            color_continuous_scale='Blues',
            aspect='auto'
        )
        st.plotly_chart(fig2, use_container_width=True)

# ── Page 3: Pipeline Summary ──────────────────
elif page == "🔧 Pipeline Summary":
    st.title("🔧 Pipeline Summary")
    st.markdown("Hourly overview of total GitHub activity processed by the pipeline.")

    df = run_query("""
        SELECT HOUR, TOTAL_STARS, TOTAL_PUSHES, TOTAL_EVENTS, STAR_PCT
        FROM github_analytics.ANALYTICS.PIPELINE_SUMMARY
        ORDER BY HOUR DESC
        LIMIT 48
    """)

    if df.empty:
        st.warning("No pipeline data found.")
    else:
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Hours Processed", len(df))
        col2.metric("Total Stars", f"{int(df['TOTAL_STARS'].sum()):,}")
        col3.metric("Total Events", f"{int(df['TOTAL_EVENTS'].sum()):,}")

        fig = px.line(
            df.sort_values('HOUR'),
            x='HOUR',
            y=['TOTAL_STARS', 'TOTAL_PUSHES'],
            title='Stars and Pushes Over Time',
            labels={'value': 'Count', 'HOUR': 'Hour (UTC)'}
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

        st.markdown("### Raw Data")
        st.dataframe(df, use_container_width=True)

st.sidebar.markdown("---")
st.sidebar.markdown(f"Last refreshed: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")