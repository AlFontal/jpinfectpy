
import polars as pl

import jpinfectpy as jp

print("--- Testing jp.load_all() ---")

try:
    # This triggers download_recent(), which might take time or fail if no internet/404s
    # But checking if logic runs.
    df = jp.load_all(return_type="polars")

    print(f"Shape: {df.shape}")
    print("Columns:", df.columns)
    print("Sources:", df["source"].unique().to_list())

    # Check historical part
    hist = df.filter(pl.col("source") == "historical_sex")
    print(f"Historical rows: {len(hist)}")

    # Check recent part (if any downloaded)
    recent = df.filter(pl.col("source") == "recent_bullet")
    print(f"Recent rows: {len(recent)}")

    if len(recent) > 0:
        print("Recent head:\n", recent.head())

except Exception as e:
    print(f"FAILED: {e}")
    import traceback

    traceback.print_exc()
