from __future__ import annotations

import polars as pl

from .types import AnyFrame, ReturnType
from .utils import resolve_return_type, to_pandas, to_polars


def _infer_dataset_type(df: pl.DataFrame) -> str:
    cols = df.columns
    if "disease" in cols and "cases" in cols:
        return "long"
    lowered = [c.lower() for c in cols]
    if any("weekly" in c or "cumulative" in c or "total" in c for c in lowered):
        return "bullet"
    col_count = df.width - 4
    if col_count > 0 and col_count % 3 == 0:
        return "sex"
    if col_count > 0 and col_count % 4 == 0:
        return "place"
    return "unknown"


def _col_join_rename(df: pl.DataFrame) -> pl.DataFrame:
    dataset_type = _infer_dataset_type(df)
    mapping: dict[str, str] = {}
    if dataset_type == "place":
        for name in df.columns:
            mapping[name] = name.replace("Unknown", "Unknown place").replace(
                "Others", "Other places"
            )
    elif dataset_type == "bullet":
        for name in df.columns:
            mapping[name] = name.replace("weekly", "total")
    if mapping:
        return df.rename(mapping)
    return df


def merge(*dfs: AnyFrame, return_type: ReturnType | None = None) -> AnyFrame:
    if len(dfs) < 2:
        raise ValueError("merge requires at least two dataframes")

    polars_frames = [_col_join_rename(to_polars(df)) for df in dfs]

    key_cols = ["prefecture", "year", "week", "date"]
    merged = polars_frames[0]
    merged = merged.join(polars_frames[1], on=key_cols, how="full")

    if len(polars_frames) > 2:
        merged = pl.concat([merged, *polars_frames[2:]], how="diagonal_relaxed")

    if resolve_return_type(return_type) == "pandas":
        return to_pandas(merged)
    return merged


def pivot(df: AnyFrame, return_type: ReturnType | None = None) -> AnyFrame:
    frame = to_polars(df)
    key_cols = ["prefecture", "year", "week", "date"]
    cols = set(frame.columns)
    if "disease" in cols and "cases" in cols:
        result = frame.pivot(values="cases", index=key_cols, on="disease")
    else:
        missing = [col for col in key_cols if col not in cols]
        if missing:
            missing_labels = ", ".join(missing)
            raise ValueError(
                "pivot expects either long-form data with 'disease' and 'cases' "
                f"or wide-form data with key columns. Missing: {missing_labels}"
            )
        result = frame.unpivot(index=key_cols, variable_name="disease", value_name="cases")
    if resolve_return_type(return_type) == "pandas":
        return to_pandas(result)
    return result
