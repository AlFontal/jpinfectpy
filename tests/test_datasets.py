from __future__ import annotations

import polars as pl

from jpinfectpy.datasets import load_dataset


def test_load_dataset() -> None:
    df = load_dataset("sex_prefecture", return_type="polars")
    assert isinstance(df, pl.DataFrame)
    assert df.height > 0
