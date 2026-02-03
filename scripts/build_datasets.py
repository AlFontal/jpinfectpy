#!/usr/bin/env python3
"""
Script to build bundled datasets for jpinfectpy.
This downloads historical data and saves it as Parquet files in the package.
"""

from pathlib import Path
import shutil

import polars as pl
from jpinfectpy import io

DATA_DIR = Path(__file__).parent.parent / "src" / "jpinfectpy" / "data"


def build_sex():
    print("Building sex_prefecture dataset...")
    years = range(1999, 2024)
    dfs = []
    for year in years:
        try:
            path = io.download("sex", year)
            df = io.read(path, type="sex", return_type="polars")
            dfs.append(df)
            print(f"  Loaded {year}")
        except Exception as e:
            print(f"  Failed {year}: {e}")

    if dfs:
        full_df = pl.concat(dfs, how="diagonal_relaxed")
        out_path = DATA_DIR / "sex_prefecture.parquet"
        full_df.write_parquet(out_path)
        print(f"Saved to {out_path} ({full_df.height} rows)")


def build_place():
    print("\nBuilding place_prefecture dataset...")
    years = range(2001, 2024)
    dfs = []
    for year in years:
        try:
            path = io.download("place", year)
            df = io.read(path, type="place", return_type="polars")
            dfs.append(df)
            print(f"  Loaded {year}")
        except Exception as e:
            print(f"  Failed {year}: {e}")

    if dfs:
        full_df = pl.concat(dfs, how="diagonal_relaxed")
        out_path = DATA_DIR / "place_prefecture.parquet"
        full_df.write_parquet(out_path)
        print(f"Saved to {out_path} ({full_df.height} rows)")


def build_bullet():
    print("\nBuilding bullet dataset (2024-2025)...")
    # Fetch recent years
    years = [2024, 2025]
    dfs = []
    for year in years:
        try:
            # Try to download all weeks
            paths = io.download("bullet", year, week=range(1, 54))
            if not paths:
                continue
            # Read folder or specific files?
            # io.download for bullet returns list[Path].
            # io.read handles file or directory.
            # We can read file by file to be safe or just pass the directory?
            # io.read(path_to_one_file) handles that file.
            # But io.read(directory) reads all csvs in it.
            # We want only the ones we just downloaded?
            # io.read_bullet_pl logic: if directory is passed it reads *.csv.
            # Let's read each file.
            if isinstance(paths, list):
                for p in paths:
                    df = io.read(p, type="bullet", return_type="polars")
                    dfs.append(df)
                print(f"  Loaded {len(paths)} weeks for {year}")
        except Exception as e:
            print(f"  Failed {year}: {e}")

    if dfs:
        full_df = pl.concat(dfs, how="diagonal_relaxed")
        out_path = DATA_DIR / "bullet.parquet"
        full_df.write_parquet(out_path)
        print(f"Saved to {out_path} ({full_df.height} rows)")


def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    build_sex()
    build_place()
    build_bullet()


if __name__ == "__main__":
    main()
