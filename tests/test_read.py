from __future__ import annotations

from pathlib import Path

import polars as pl

from jpinfectpy.io_bullet import read_bullet_pl
from jpinfectpy.io_confirmed import read_confirmed_pl

FIXTURES = Path(__file__).parent / "fixtures"


def test_read_confirmed_pl() -> None:
    path = FIXTURES / "Syu_01_1_2024.xlsx"
    df = read_confirmed_pl(path, type="sex")
    assert isinstance(df, pl.DataFrame)
    assert {"prefecture", "year", "week", "date"}.issubset(set(df.columns))
    assert df.height > 0


def test_read_bullet_pl() -> None:
    df = read_bullet_pl(FIXTURES, year=2024, week=[1], lang="en")
    assert isinstance(df, pl.DataFrame)
    assert {"prefecture", "year", "week", "date"}.issubset(set(df.columns))
    assert df.height == 2
