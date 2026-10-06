import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import os

# ============================================
# GITHUB TREND INTELLIGENCE DASHBOARD
# Reads from local Parquet files (DuckDB)
# ============================================

st.set_page_config(
    page_title="GitHub Trend Intelligence",
    page_icon="🔥",
    layout="wide"
)

# ── Data directory ────────────────────────
DATA_DIR = "data"

# ── DuckDB connection ─────────────────────
@st.cache_resource
def get_duckdb_conn():
    """Create a DuckDB connection."""
    return duckdb.connect(':memory:')

@st.cache_data(ttl=300)
def load_data(table_name):
    """Load parquet data into a DataFrame."""
    parquet_file = os.path.join(DATA_DIR, f"{table_name}.parquet")

    if not os.path.exists(parquet_file):
        st.warning(f"⚠️ Data file not found: {parquet_file}")
        st.info("Run the Airflow pipeline to generate data: `airflow trigger_dag github_pipeline_full`")
        return pd.DataFrame()

    try:
        # Use DuckDB to read parquet
        conn = get_duckdb_conn()
        df = conn.execute(f"SELECT * FROM read_parquet('{parquet_file}')").fetchdf()
        return df
    except Exception as e:
        st.error(f"Error loading {table_name}: {e}")
        return pd.DataFrame()

# ── Sidebar ───────────────────────────────
st.sidebar.title("GitHub Trend Intelligence")
st.sidebar.markdown("Real-time GitHub activity analytics powered by a full medallion architecture pipeline.")
st.sidebar.markdown("---")
st.sidebar.markdown("**Stack**")
st.sidebar.markdown("Airflow · Databricks · Delta Lake · **DuckDB** · Streamlit")
st.sidebar.markdown("---")

page = st.sidebar.radio(
    "Navigate",
    ["🔥 Trending Repos", "📊 Event Activity", "🔧 Pipeline Summary"]
)

# ── Page 1: Trending Repos ────────────────────
if page == "🔥 Trending Repos":
    st.title("🔥 Trending GitHub Repositories")
    st.markdown("Repositories with unusual star velocity detected by z-score anomaly detection.")

    df = load_data("gold_star_counts")

    if df.empty:
        st.warning("No trending repos found. Run the pipeline to load data.")
    else:
        # Real columns: repo_name, hour, star_count
        # Aggregate by repo to get total stars
        repo_stars = df.groupby('repo_name')['star_count'].sum().reset_index()
        repo_stars = repo_stars.sort_values('star_count', ascending=False)

        col1, col2, col3 = st.columns(3)
        col1.metric("Total Repos Tracked", len(repo_stars))
        col2.metric("Top Repo Stars", int(repo_stars['star_count'].max()) if len(repo_stars) > 0 else 0)
        col3.metric("Avg Stars/Repo", round(repo_stars['star_count'].mean(), 1) if len(repo_stars) > 0 else 0)

        st.markdown("### Top Trending Repos")
        fig = px.bar(
            repo_stars.head(15),
            x='star_count',
            y='repo_name',
            orientation='h',
            color='star_count',
            color_continuous_scale='Blues',
            title='Top 15 Repos by Star Count',
            labels={'star_count': 'Total Stars', 'repo_name': 'Repository'}
        )
        fig.update_layout(yaxis={'categoryorder': 'total ascending'}, height=500)
        st.plotly_chart(fig, use_container_width=True)

        st.markdown("### Full Trending Table")
        st.dataframe(
            repo_stars[['repo_name', 'star_count']].head(30),
            use_container_width=True
        )

# ── Page 2: Event Activity ────────────────────
elif page == "📊 Event Activity":
    st.title("📊 GitHub Event Activity by Hour")
    st.markdown("Shows which hours of the day are most active per event type (UTC).")

    df = load_data("gold_event_summary")

    if df.empty:
        st.warning("No event data found.")
    else:
        # Real columns: event_type, hour, event_count
        if 'event_type' in df.columns and 'hour' in df.columns and 'event_count' in df.columns:
            event_types = df['event_type'].unique().tolist()
            selected = st.multiselect(
                "Select event types",
                event_types,
                default=event_types[:4]
            )

            filtered = df[df['event_type'].isin(selected)].copy()

            # Extract hour of day for chart
            filtered['hour_of_day'] = pd.to_datetime(filtered['hour']).dt.hour
            hourly_data = filtered.groupby(['event_type', 'hour_of_day'])['event_count'].sum().reset_index()

            fig = px.line(
                hourly_data,
                x='hour_of_day',
                y='event_count',
                color='event_type',
                title='GitHub Events by Hour of Day (UTC)',
                labels={'hour_of_day': 'Hour (UTC)', 'event_count': 'Total Events'}
            )
            fig.update_layout(height=450)
            st.plotly_chart(fig, use_container_width=True)

            st.markdown("### Heatmap")
            pivot = hourly_data.pivot_table(
                index='event_type',
                columns='hour_of_day',
                values='event_count',
                fill_value=0
            )
            fig2 = px.imshow(
                pivot,
                title='Event Heatmap by Type and Hour',
                color_continuous_scale='Blues',
                aspect='auto'
            )
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.error("Missing required columns: event_type, hour, event_count")

# ── Page 3: Pipeline Summary ──────────────────
elif page == "🔧 Pipeline Summary":
    st.title("🔧 Pipeline Summary")
    st.markdown("Hourly overview of total GitHub activity processed by the pipeline.")

    df = load_data("gold_push_activity")

    if df.empty:
        st.warning("No pipeline data found.")
    else:
        # Real columns: repo_name, hour, push_count
        if 'hour' in df.columns and 'push_count' in df.columns:
            # Aggregate by hour
            hourly = df.groupby('hour')['push_count'].sum().reset_index()
            hourly = hourly.sort_values('hour', ascending=False).head(48)

            col1, col2, col3 = st.columns(3)
            col1.metric("Total Hours Processed", len(hourly))
            col2.metric("Total Pushes", f"{int(hourly['push_count'].sum()):,}")
            col3.metric("Avg Pushes/Hour", round(hourly['push_count'].mean(), 1) if len(hourly) > 0 else 0)

            fig = px.line(
                hourly.sort_values('hour'),
                x='hour',
                y='push_count',
                title='Push Events Over Time',
                labels={'push_count': 'Push Count', 'hour': 'Hour (UTC)'}
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

            st.markdown("### Raw Data")
            st.dataframe(hourly, use_container_width=True)
        else:
            st.error("Missing required columns: hour, push_count")

st.sidebar.markdown("---")
st.sidebar.markdown(f"Last refreshed: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
st.sidebar.info("💡 Data is updated hourly by the Airflow pipeline running on your Databricks cluster.")
