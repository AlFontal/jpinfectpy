from __future__ import annotations

import datetime as dt
import re
from collections.abc import Iterable
from pathlib import Path
from typing import Literal

import polars as pl

from .config import get_config
from .http import download_urls
from .types import AnyFrame, ReturnType
from .urls import url_bullet
from .utils import resolve_return_type, to_pandas


def _col_rename_bullet(names: list[str]) -> list[str]:
    cleaned: list[str] = []
    for name in names:
        name = re.sub(r"^.*[\r\n]+", "", str(name))
        name = re.sub(r"^.*[\r\n]+", "", name)
        name = re.sub(r"^\.\.\.[0-9]+$", "", name)
        name = name.replace("Ｉ", "I")  # noqa: RUF001
        name = name.replace("（", "(").replace("）", ")")  # noqa: RUF001
        name = re.sub(r"\s+", " ", name).strip()
        name = re.sub(r"^\(", "", name)
        name = re.sub(r"\)$", "", name)
        if name:
            cleaned.append(name)
    return cleaned


def _extract_year_week(path: Path) -> tuple[int | None, int | None]:
    year_match = re.search(r"(19|20)\d{2}", path.name)
    week_match = re.search(r"(?:-)(\d{2})|zensu(\d{2})", path.name)
    year = int(year_match.group(0)) if year_match else None
    week = None
    if week_match:
        week = int(week_match.group(1) or week_match.group(2))
    return year, week


def _iso_week_date(year: int, week: int) -> dt.date | None:
    try:
        return dt.date.fromisocalendar(int(year), int(week), 7)
    except Exception:
        return None


def read_bullet_pl(
    directory: Path | None = None,
    *,
    year: int | None = None,
    week: Iterable[int] | None = None,
    lang: Literal["en", "ja"] = "en",
) -> pl.DataFrame:
    config = get_config()

    if directory is None:
        if year is None:
            raise ValueError("year must be provided when directory is None")
        cache_dir = config.cache_dir / "raw" / "bullet"
        paths = get_bullet(year, week, cache_dir, lang=lang)
    else:
        paths = list(directory.glob("*.csv"))

    if week is not None:
        week_set = {int(w) for w in week}
        paths = [p for p in paths if (_extract_year_week(p)[1] in week_set)]

    frames: list[pl.DataFrame] = []
    for path in sorted(paths):
        df = pl.read_csv(path, infer_schema_length=0)
        cleaned = _col_rename_bullet(df.columns)
        if len(cleaned) == len(df.columns):
            df = df.rename(dict(zip(df.columns, cleaned, strict=True)))
        if df.columns[0] != "prefecture":
            df = df.rename({df.columns[0]: "prefecture"})

        file_year, file_week = _extract_year_week(path)
        y = year or file_year
        w = file_week
        if y is None or w is None:
            continue

        if "year" not in df.columns:
            df = df.with_columns(pl.lit(y).alias("year"))
        if "week" not in df.columns:
            df = df.with_columns(pl.lit(w).alias("week"))
        if "date" not in df.columns:
            df = df.with_columns(
                pl.struct(["year", "week"])
                .map_elements(
                    lambda x: _iso_week_date(int(x["year"]), int(x["week"])),
                    return_dtype=pl.Date,
                )
                .alias("date")
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
        frames.append(df)

    if not frames:
        return pl.DataFrame()
    return pl.concat(frames, how="vertical")


def read_bullet(
    directory: Path | None = None,
    *,
    year: int | None = None,
    week: Iterable[int] | None = None,
    lang: Literal["en", "ja"] = "en",
    return_type: ReturnType | None = None,
) -> AnyFrame:
    df = read_bullet_pl(directory, year=year, week=week, lang=lang)
    if resolve_return_type(return_type) == "pandas":
        return to_pandas(df)
    return df


def get_bullet(
    year: int,
    week: int | Iterable[int] | None,
    out_dir: Path,
    *,
    lang: Literal["en", "ja"] = "en",
    overwrite: bool = False,
) -> list[Path]:
    urls = url_bullet(year, week, lang=lang)
    if not urls:
        return []
    out_dir.mkdir(parents=True, exist_ok=True)
    existing = {p.name: p for p in out_dir.glob("*.csv")}

    if overwrite:
        return download_urls(urls, out_dir, get_config())

    needed = [url for url in urls if Path(url).name not in existing]
    if not needed:
        return [existing[Path(url).name] for url in urls]

    downloaded = download_urls(needed, out_dir, get_config())
    downloaded_map = {p.name: p for p in downloaded}
    results: list[Path] = []
    for url in urls:
        name = Path(url).name
        if name in existing:
            results.append(existing[name])
        elif name in downloaded_map:
            results.append(downloaded_map[name])
    return results
