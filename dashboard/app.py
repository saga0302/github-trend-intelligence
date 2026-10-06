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

# ── Data directory ────────────────────────────
DATA_DIR = "data"  # Local parquet files stored here

# ── DuckDB connection ─────────────────────────
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

# ── Sidebar ───────────────────────────────────
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

    df = load_data("trending_repos")

    if df.empty:
        st.warning("No trending repos found. Run the pipeline to load data.")
    else:
        # Normalize column names (Parquet might have case variations)
        df.columns = df.columns.str.lower()

        col1, col2, col3 = st.columns(3)
        col1.metric("Total Repos Tracked", len(df))
        col2.metric("Top Repo Stars", int(df['recent_stars'].max()) if 'recent_stars' in df.columns else 0)
        col3.metric("Avg Stars/Repo", round(df['recent_stars'].mean(), 1) if 'recent_stars' in df.columns else 0)

        st.markdown("### Top Trending Repos")
        fig = px.bar(
            df.head(15),
            x='recent_stars',
            y='repo_name',
            orientation='h',
            color='recent_stars',
            color_continuous_scale='Blues',
            title='Top 15 Repos by Star Count'
        )
        fig.update_layout(yaxis={'categoryorder': 'total ascending'}, height=500)
        st.plotly_chart(fig, use_container_width=True)

        st.markdown("### Full Trending Table")
        display_cols = ['repo_name', 'recent_stars', 'avg_hourly_stars', 'z_score', 'latest_hour']
        display_cols = [col for col in display_cols if col in df.columns]
        st.dataframe(
            df[display_cols],
            use_container_width=True
        )

# ── Page 2: Event Activity ────────────────────
elif page == "📊 Event Activity":
    st.title("📊 GitHub Event Activity by Hour")
    st.markdown("Shows which hours of the day are most active per event type (UTC).")

    df = load_data("language_activity")

    if df.empty:
        st.warning("No event data found.")
    else:
        df.columns = df.columns.str.lower()

        if 'event_type' in df.columns and 'event_hour' in df.columns:
            event_types = df['event_type'].unique().tolist()
            selected = st.multiselect(
                "Select event types",
                event_types,
                default=event_types[:4]
            )

            filtered = df[df['event_type'].isin(selected)]

            fig = px.line(
                filtered,
                x='event_hour',
                y='total_events',
                color='event_type',
                title='GitHub Events by Hour of Day (UTC)',
                labels={'event_hour': 'Hour (UTC)', 'total_events': 'Total Events'}
            )
            fig.update_layout(height=450)
            st.plotly_chart(fig, use_container_width=True)

            st.markdown("### Heatmap")
            pivot = filtered.pivot_table(
                index='event_type',
                columns='event_hour',
                values='total_events',
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
            st.error("Missing required columns: event_type, event_hour")

# ── Page 3: Pipeline Summary ──────────────────
elif page == "🔧 Pipeline Summary":
    st.title("🔧 Pipeline Summary")
    st.markdown("Hourly overview of total GitHub activity processed by the pipeline.")

    df = load_data("pipeline_summary")

    if df.empty:
        st.warning("No pipeline data found.")
    else:
        df.columns = df.columns.str.lower()

        if 'hour' in df.columns:
            col1, col2, col3 = st.columns(3)
            col1.metric("Total Hours Processed", len(df))
            col2.metric("Total Stars", f"{int(df['total_stars'].sum()):,}" if 'total_stars' in df.columns else 0)
            col3.metric("Total Events", f"{int(df['total_events'].sum()):,}" if 'total_events' in df.columns else 0)

            fig = px.line(
                df.sort_values('hour'),
                x='hour',
                y=['total_stars', 'total_pushes'] if 'total_pushes' in df.columns else ['total_stars'],
                title='Stars and Pushes Over Time',
                labels={'value': 'Count', 'hour': 'Hour (UTC)'}
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

            st.markdown("### Raw Data")
            st.dataframe(df, use_container_width=True)
        else:
            st.error("Missing required columns for pipeline summary")

st.sidebar.markdown("---")
st.sidebar.markdown(f"Last refreshed: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
st.sidebar.info("💡 Data is updated hourly by the Airflow pipeline running on your Databricks cluster.")
