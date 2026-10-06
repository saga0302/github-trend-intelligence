#!/usr/bin/env python3

import os
import pandas as pd
from databricks import sql
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# ============================================
# CONFIGURATION
# ============================================

# Your Databricks credentials (from .env file)
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

# Output directory for Parquet files
DATA_DIR = "data"

# Gold tables to export
GOLD_TABLES = {
    "gold_star_counts": "default.gold_star_counts",
    "gold_event_summary": "default.gold_event_summary",
    "gold_push_activity": "default.gold_push_activity",
}

# ============================================
# EXPORT FUNCTION
# ============================================

def export_gold_tables():
    """Connect to Databricks and export Gold tables to Parquet."""

    print("=" * 60)
    print("Databricks Gold Tables → Parquet Export")
    print("=" * 60)

    # Create data directory
    os.makedirs(DATA_DIR, exist_ok=True)
    print(f"\n📁 Output directory: {DATA_DIR}/\n")

    # Validate credentials
    if not DATABRICKS_TOKEN or not DATABRICKS_HOST:
        print("❌ ERROR: Databricks credentials not found!")
        print("   Make sure .env file contains DATABRICKS_TOKEN, DATABRICKS_HOST, and DATABRICKS_HTTP_PATH")
        return False

    try:
        # Connect to Databricks
        print("🔌 Connecting to Databricks...")
        conn = sql.connect(
            server_hostname=DATABRICKS_HOST,
            http_path=DATABRICKS_HTTP_PATH,
            auth_type="pat",
            token=DATABRICKS_TOKEN,
        )
        print("✅ Connected!\n")

        cursor = conn.cursor()

        # Export each Gold table
        for output_name, table_name in GOLD_TABLES.items():
            print(f"📊 Exporting {table_name}...")

            try:
                # Query the table
                cursor.execute(f"SELECT * FROM {table_name}")

                # Fetch all results as Arrow format (faster)
                result = cursor.fetchall_arrow()
                df = result.to_pandas()

                # Save to Parquet
                parquet_path = os.path.join(DATA_DIR, f"{output_name}.parquet")
                df.to_parquet(parquet_path, index=False)

                print(f"   ✅ Saved: {parquet_path}")
                print(f"   📈 Rows: {len(df)} | Columns: {len(df.columns)}\n")

            except Exception as e:
                print(f"   ❌ Error exporting {table_name}: {e}\n")
                continue

        conn.close()

        print("=" * 60)
        print("✅ Export complete!")
        print("=" * 60)
        print("\nYour Parquet files are ready in ./data/")
        print("Streamlit will auto-refresh in 5 minutes.")
        print("\nTo see changes immediately:")
        print("  1. Refresh your browser")
        print("  2. Or restart Streamlit: python3 -m streamlit run dashboard/app.py")

        return True

    except Exception as e:
        print(f"\n❌ Connection failed: {e}")
        print("\nCommon issues:")
        print("  • Token is invalid or expired")
        print("  • Host/HTTP path is wrong")
        print("  • Warehouse is stopped")
        return False


if __name__ == "__main__":
    success = export_gold_tables()
    exit(0 if success else 1)