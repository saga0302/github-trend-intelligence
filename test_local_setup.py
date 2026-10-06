#!/usr/bin/env python3
"""
Test script to verify the DuckDB setup works locally.
Run this before deploying to Streamlit Cloud.

Usage:
    python test_local_setup.py
"""

import os
import sys
import pandas as pd
import duckdb

DATA_DIR = "data"

def test_parquet_creation():
    """Create dummy Parquet files matching the Streamlit app's expectations."""
    print("\n📝 Creating dummy Parquet files for testing...")

    os.makedirs(DATA_DIR, exist_ok=True)

    # Create trending_repos
    trending_repos_df = pd.DataFrame({
        'repo_name': ['torvalds/linux', 'facebook/react', 'microsoft/vscode', 'golang/go'],
        'recent_stars': [100, 200, 150, 120],
        'avg_hourly_stars': [10, 20, 15, 12],
        'stddev_stars': [2, 4, 3, 2.5],
        'z_score': [45.0, 45.0, 45.0, 40.0],
        'hours_observed': [24, 24, 24, 24],
        'latest_hour': ['2024-01-01 12:00:00'] * 4
    })
    trending_repos_df.to_parquet(f"{DATA_DIR}/trending_repos.parquet", index=False)
    print(f"✅ Created {DATA_DIR}/trending_repos.parquet ({len(trending_repos_df)} rows)")

    # Create language_activity
    language_activity_df = pd.DataFrame({
        'event_type': ['PushEvent', 'WatchEvent', 'ForkEvent', 'PullRequestEvent'] * 6,
        'event_hour': [0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5],
        'total_events': [100, 200, 50, 75, 120, 210, 55, 80, 95, 190, 48, 70, 110, 220, 52, 78, 105, 200, 51, 76, 98, 195, 49, 72],
        'avg_events_per_hour': [10, 20, 5, 7.5] * 6
    })
    language_activity_df.to_parquet(f"{DATA_DIR}/language_activity.parquet", index=False)
    print(f"✅ Created {DATA_DIR}/language_activity.parquet ({len(language_activity_df)} rows)")

    # Create pipeline_summary
    pipeline_summary_df = pd.DataFrame({
        'hour': pd.date_range('2024-01-01', periods=48, freq='H'),
        'total_stars': [500 + i * 10 for i in range(48)],
        'total_pushes': [200 + i * 5 for i in range(48)],
        'total_events': [5000 + i * 100 for i in range(48)],
        'star_pct': [10.0 + i * 0.1 for i in range(48)]
    })
    pipeline_summary_df.to_parquet(f"{DATA_DIR}/pipeline_summary.parquet", index=False)
    print(f"✅ Created {DATA_DIR}/pipeline_summary.parquet ({len(pipeline_summary_df)} rows)")

    return True


def test_duckdb_reads():
    """Test reading Parquet files with DuckDB."""
    print("\n🔍 Testing DuckDB reads...")

    conn = duckdb.connect(':memory:')

    # Test trending_repos
    try:
        df = conn.execute(f"SELECT * FROM read_parquet('{DATA_DIR}/trending_repos.parquet')").fetchdf()
        print(f"✅ trending_repos read successfully ({len(df)} rows, {len(df.columns)} cols)")
        print(f"   Columns: {', '.join(df.columns.tolist())}")
    except Exception as e:
        print(f"❌ Failed to read trending_repos: {e}")
        return False

    # Test language_activity
    try:
        df = conn.execute(f"SELECT * FROM read_parquet('{DATA_DIR}/language_activity.parquet')").fetchdf()
        print(f"✅ language_activity read successfully ({len(df)} rows, {len(df.columns)} cols)")
        print(f"   Columns: {', '.join(df.columns.tolist())}")
    except Exception as e:
        print(f"❌ Failed to read language_activity: {e}")
        return False

    # Test pipeline_summary
    try:
        df = conn.execute(f"SELECT * FROM read_parquet('{DATA_DIR}/pipeline_summary.parquet')").fetchdf()
        print(f"✅ pipeline_summary read successfully ({len(df)} rows, {len(df.columns)} cols)")
        print(f"   Columns: {', '.join(df.columns.tolist())}")
    except Exception as e:
        print(f"❌ Failed to read pipeline_summary: {e}")
        return False

    return True


def test_duckdb_sql():
    """Test SQL queries on Parquet files."""
    print("\n📊 Testing DuckDB SQL queries...")

    conn = duckdb.connect(':memory:')

    try:
        # SQL query on trending_repos
        result = conn.execute(
            f"SELECT repo_name, recent_stars FROM read_parquet('{DATA_DIR}/trending_repos.parquet') ORDER BY recent_stars DESC LIMIT 3"
        ).fetchdf()
        print(f"✅ Top repos query returned {len(result)} rows:")
        for _, row in result.iterrows():
            print(f"   - {row['repo_name']}: {row['recent_stars']} stars")
    except Exception as e:
        print(f"❌ SQL query failed: {e}")
        return False

    try:
        # Aggregation query
        result = conn.execute(
            f"SELECT event_type, COUNT(*) as count FROM read_parquet('{DATA_DIR}/language_activity.parquet') GROUP BY event_type"
        ).fetchdf()
        print(f"✅ Event count aggregation returned {len(result)} rows")
    except Exception as e:
        print(f"❌ Aggregation query failed: {e}")
        return False

    return True


def test_directory_structure():
    """Verify directory structure is correct."""
    print("\n📁 Checking directory structure...")

    required_files = [
        "dashboard/app.py",
        "dashboard/requirements.txt",
    ]

    optional_files = [
        "dags/github_pipeline_duckdb.py",
        "export_to_parquet.py",
    ]

    for file in required_files:
        if os.path.exists(file):
            print(f"✅ {file} exists")
        else:
            print(f"⚠️  {file} missing (needed!)")
            return False

    for file in optional_files:
        if os.path.exists(file):
            print(f"✅ {file} exists")
        else:
            print(f"ℹ️  {file} not found yet (set up later)")

    return True


def main():
    """Run all tests."""
    print("=" * 60)
    print("DuckDB Migration Test Suite")
    print("=" * 60)

    # Change to repo directory if needed
    if not os.path.exists("dashboard"):
        print("⚠️  Run this from the github-trend-intelligence root directory")
        sys.exit(1)

    # Test 1: Directory structure
    if not test_directory_structure():
        print("\n❌ Directory structure check failed")
        sys.exit(1)

    # Test 2: Create dummy Parquet files
    if not test_parquet_creation():
        print("\n❌ Parquet creation failed")
        sys.exit(1)

    # Test 3: Read with DuckDB
    if not test_duckdb_reads():
        print("\n❌ DuckDB reads failed")
        sys.exit(1)

    # Test 4: SQL queries
    if not test_duckdb_sql():
        print("\n❌ DuckDB SQL queries failed")
        sys.exit(1)

    print("\n" + "=" * 60)
    print("✅ All tests passed! Ready for Streamlit deployment.")
    print("=" * 60)
    print("\nNext steps:")
    print("1. Install Streamlit: pip install streamlit")
    print("2. Run: streamlit run dashboard/app.py")
    print("3. Visit http://localhost:8501")
    print("\nWhen ready:")
    print("4. Commit Parquet files: git add data/*.parquet")
    print("5. Push to GitHub: git push")
    print("6. Deploy to Streamlit Cloud")
    print("=" * 60)


if __name__ == "__main__":
    main()
