from __future__ import annotations

import datetime as dt
import re
from collections.abc import Iterable
from pathlib import Path
from typing import Literal

import pandas as pd
import polars as pl

from .config import get_config
from .datasets import load_prefecture_en
from .http import download_urls
from .types import AnyFrame, ReturnType
from .urls import url_confirmed
from .utils import resolve_return_type, to_pandas


def _col_rename(names: list[str], rep_each: int = 1) -> list[str]:
    cleaned: list[str] = []
    for name in names:
        name = re.sub(r"^.*[\r\n]+", "", str(name))
        name = re.sub(r"^.*[\r\n]+", "", name)
        name = re.sub(r"^\.\.\.[0-9]+$", "", name)
        name = name.replace("Ｉ", "I")  # noqa: RUF001
        name = name.replace("（", "(").replace("）", ")")  # noqa: RUF001
        if name == "Pandemic influenza (A/H1N1)":
            name = "(Pandemic influenza (A/H1N1))"
        name = name.strip()
        name = re.sub(r"^\(", "", name)
        name = re.sub(r"\)$", "", name)
        name = re.sub(r"([a-zA-Z])\(", r"\1 (", name)
        if name:
            cleaned.append(name)
    if rep_each > 1:
        expanded: list[str] = []
        for item in cleaned:
            expanded.extend([item] * rep_each)
        return expanded
    return cleaned


def _read_excel_sheets(
    file_path: Path, sheet_range: Iterable[int]
) -> list[tuple[int, pd.DataFrame]]:
    frames: list[tuple[int, pd.DataFrame]] = []
    for sheet in sheet_range:
        df = None
        try:
            df = pd.read_excel(file_path, sheet_name=sheet, engine="openpyxl")
        except ValueError:
            try:
                df = pd.read_excel(file_path, sheet_name=str(sheet), engine="openpyxl")
            except ValueError:
                continue
        df = df.dropna(axis=1, how="all")
        df = df.dropna(axis=0, how="all")
        if not df.empty:
            frames.append((sheet, df))
    return frames


def _infer_year_from_path(path: Path) -> int | None:
    match = re.search(r"(19|20)\d{2}", path.name)
    if not match:
        return None
    return int(match.group(0))


def _sheet_range_for_year(year: int) -> range:
    if year == 1999:
        return range(2, 41)
    if year in {2004, 2009, 2015}:
        return range(2, 55)
    return range(2, 54)


def _combine_confirmed_frames(
    frames: list[tuple[int, pd.DataFrame]],
    year: int,
    *,
    week_offset: int,
) -> pl.DataFrame:
    combined: list[pl.DataFrame] = []
    for sheet, df in frames:
        week = sheet + week_offset
        df = df.copy()
        df["year"] = year
        df["week"] = week
        combined.append(pl.from_pandas(df))
    if not combined:
        return pl.DataFrame()
    return pl.concat(combined, how="vertical")


def _ensure_prefecture_column(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty():
        return df
    if df.columns[0] != "prefecture":
        df = df.rename({df.columns[0]: "prefecture"})
    prefecture = df.get_column("prefecture")
    if prefecture.dtype == pl.Utf8:
        return df
    try:
        prefecture_list = load_prefecture_en()
    except Exception:
        return df
    block = len(prefecture_list) + 1
    if df.height % block == 0:
        repeats = df.height // block
        names = ["Total", *prefecture_list] * repeats
        return df.with_columns(pl.Series("prefecture", names))
    return df


def read_confirmed_pl(
    path: Path,
    *,
    type: Literal["sex", "place"] | None = None,
) -> pl.DataFrame:
    if path.is_dir():
        if type == "sex":
            pattern = re.compile(r"(Syu_01_1|01_1)\.(xls|xlsx)$")
        elif type == "place":
            pattern = re.compile(r"(Syu_02_1|02_1)\.(xls|xlsx)$")
        else:
            pattern = re.compile(r"Syu_0[12]_1\.(xls|xlsx)$")
        files = [p for p in path.iterdir() if pattern.search(p.name)]
    else:
        files = [path]

    frames: list[pl.DataFrame] = []
    for file_path in sorted(files):
        year = _infer_year_from_path(file_path) or 0
        if year == 0:
            continue
        sheet_range = _sheet_range_for_year(year)
        excel_frames = _read_excel_sheets(file_path, sheet_range)
        week_offset = 12 if year == 1999 else -1
        combined = _combine_confirmed_frames(excel_frames, year, week_offset=week_offset)
        combined = _ensure_prefecture_column(combined)
        frames.append(combined)

    if not frames:
        return pl.DataFrame()

    df = pl.concat(frames, how="vertical")
    df = df.rename({df.columns[0]: "prefecture"})

    # Clean column names (best effort)
    cleaned_names = _col_rename(df.columns)
    if len(cleaned_names) == len(df.columns):
        df = df.rename(dict(zip(df.columns, cleaned_names, strict=True)))
    else:
        # Preserve key columns if rename count mismatches
        key_cols = ["prefecture", "year", "week"]
        for key in key_cols:
            if key not in df.columns:
                continue
        df.columns = [str(c) for c in df.columns]

    if "date" not in df.columns and "year" in df.columns and "week" in df.columns:
        df = df.with_columns(
            pl.struct(["year", "week"])
            .map_elements(
                lambda x: _iso_week_date(x["year"], x["week"]),
                return_dtype=pl.Date,
            )
            .alias("date")
        )

    df = df.with_columns(
        [
            pl.col(col).cast(pl.Float64, strict=False)
            for col in df.columns
            if col not in {"prefecture", "year", "week", "date"}
        ]
    )
    df = df.select(
        [
            "prefecture",
            "year",
            "week",
            "date",
            *[c for c in df.columns if c not in {"prefecture", "year", "week", "date"}],
        ]
    )
    return df


def _iso_week_date(year: int, week: int) -> dt.date | None:
    try:
        return dt.date.fromisocalendar(int(year), int(week), 7)
    except Exception:
        return None


def read_confirmed(
    path: Path,
    *,
    type: Literal["sex", "place"] | None = None,
    return_type: ReturnType | None = None,
) -> AnyFrame:
    df = read_confirmed_pl(path, type=type)
    if resolve_return_type(return_type) == "pandas":
        return to_pandas(df)
    return df


def get_confirmed(
    year: int,
    type: Literal["sex", "place"],
    out_dir: Path,
    *,
    overwrite: bool = False,
) -> Path:
    if type == "sex" and not (1999 <= year <= 2023):
        raise ValueError("year must be between 1999 and 2023 for sex data")
    if type == "place" and not (2001 <= year <= 2023):
        raise ValueError("year must be between 2001 and 2023 for place data")

    out_dir.mkdir(parents=True, exist_ok=True)
    url = url_confirmed(year, type)
    filename = f"{year}_{Path(url).name}"
    dest = out_dir / filename

    if dest.exists() and not overwrite:
        return dest

    config = get_config()
    downloaded = download_urls([url], out_dir, config)
    if not downloaded:
        raise RuntimeError("Failed to download confirmed data")
    return downloaded[0]
