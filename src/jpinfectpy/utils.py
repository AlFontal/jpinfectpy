from __future__ import annotations

from collections.abc import Iterable

import pandas as pd
import polars as pl

from .config import get_config
from .types import AnyFrame, ReturnType


def resolve_return_type(return_type: ReturnType | None) -> ReturnType:
    if return_type is not None:
        return return_type
    return get_config().return_type


def to_polars(df: AnyFrame) -> pl.DataFrame:
    if isinstance(df, pl.DataFrame):
        return df
    if isinstance(df, pd.DataFrame):
        return pl.from_pandas(df)
    raise TypeError(f"Unsupported frame type: {type(df)!r}")


def to_pandas(df: pl.DataFrame) -> pd.DataFrame:
    return df.to_pandas()


def ensure_polars(frames: Iterable[AnyFrame]) -> list[pl.DataFrame]:
    return [to_polars(frame) for frame in frames]
