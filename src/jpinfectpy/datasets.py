from __future__ import annotations

from importlib import resources
from pathlib import Path
from typing import Literal

import polars as pl

from .types import AnyFrame, DatasetName, ReturnType
from .utils import resolve_return_type, to_pandas

_DATASETS = {
    "sex_prefecture": "sex_prefecture.parquet",
    "place_prefecture": "place_prefecture.parquet",
    "bullet": "bullet.parquet",
    "prefecture_en": "prefecture_en.parquet",
}


def _data_path(name: str) -> Path:
    try:
        filename = _DATASETS[name]
    except KeyError as exc:
        raise ValueError(f"Unknown dataset: {name}") from exc
    return Path(str(resources.files("jpinfectpy.data").joinpath(filename)))


def load_dataset(
    name: DatasetName | Literal["sex_prefecture", "place_prefecture"],
    *,
    return_type: ReturnType | None = None,
) -> AnyFrame:
    """
    Load a bundled dataset.

    Args:
        name: "sex", "place", or "bullet".
              "sex_prefecture" and "place_prefecture" are aliases.
    """
    if name == "sex":
        name = "sex_prefecture"
    elif name == "place":
        name = "place_prefecture"

    path = _data_path(name)
    df = pl.read_parquet(path)
    if resolve_return_type(return_type) == "pandas":
        return to_pandas(df)
    return df


def load_all(
    *,
    return_type: ReturnType | None = None,
) -> AnyFrame:
    """
    Load a fused dataset combining historical sex-disaggregated data (1999-2023)
    and recent weekly reports (2024-present).

    The resulting DataFrame includes a 'source' column.
    Note: Recent data does not have sex disaggregation and is treated as 'total'.
    """
    from .io import download_recent, read

    # 1. Load Historical Data (Sex)
    # Filter to 'total' category to match bullet data granularity
    hist_df = load_dataset("sex", return_type="polars")
    hist_df = hist_df.filter(pl.col("category") == "total")
    hist_df = hist_df.with_columns(pl.lit("historical_sex").alias("source"))

    # 2. Load Recent Data (Bullet)
    # Download 2024+
    bullet_paths = download_recent()
    recent_dfs = []

    # If no recent data, return history only
    if not bullet_paths:
        if resolve_return_type(return_type) == "pandas":
            return to_pandas(hist_df)
        return hist_df

    # Read and normalize recent data
    for p in bullet_paths:
        try:
            df = read(p, type="bullet", return_type="polars")
            # Bullet data: prefecture, year, week, date, disease, count
            # Needs 'category' column to match history
            if "category" not in df.columns:
                df = df.with_columns(pl.lit("total").alias("category"))

            df = df.with_columns(pl.lit("recent_bullet").alias("source"))

            # Align columns if needed
            cols = ["prefecture", "year", "week", "date", "disease", "category", "count", "source"]
            df = df.select([c for c in cols if c in df.columns])
            recent_dfs.append(df)
        except Exception:
            continue

    if recent_dfs:
        recent_all = pl.concat(recent_dfs, how="vertical_relaxed")
        # Combine
        combined = pl.concat([hist_df, recent_all], how="diagonal_relaxed")
    else:
        combined = hist_df

    # Sort
    combined = combined.sort(["year", "week", "prefecture"])

    if resolve_return_type(return_type) == "pandas":
        return to_pandas(combined)
    return combined


def load_prefecture_en() -> list[str]:
    path = _data_path("prefecture_en")
    df = pl.read_parquet(path)
    return df.get_column("prefecture").to_list()
