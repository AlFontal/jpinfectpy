from __future__ import annotations

from importlib import resources
from pathlib import Path
from typing import Literal

import polars as pl

from .types import AnyFrame, ReturnType
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
    name: Literal["sex_prefecture", "place_prefecture", "bullet"],
    *,
    return_type: ReturnType | None = None,
) -> AnyFrame:
    path = _data_path(name)
    df = pl.read_parquet(path)
    if resolve_return_type(return_type) == "pandas":
        return to_pandas(df)
    return df


def load_prefecture_en() -> list[str]:
    path = _data_path("prefecture_en")
    df = pl.read_parquet(path)
    return df.get_column("prefecture").to_list()
