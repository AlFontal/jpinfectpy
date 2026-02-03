from __future__ import annotations

import datetime as dt
import re
from collections.abc import Iterable
from pathlib import Path
from typing import Literal

import polars as pl
from platformdirs import user_cache_dir

from .config import get_config
from .datasets import load_prefecture_en
from .http import download_urls
from .types import AnyFrame, DatasetName, ReturnType
from .urls import url_bullet, url_confirmed
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


def _read_excel_sheets(
    file_path: Path, sheet_range: Iterable[int]
) -> list[tuple[int, pl.DataFrame]]:
    frames: list[tuple[int, pl.DataFrame]] = []
    # Convert path to string for fastexcel compatibility/safety
    path_str = str(file_path)

    for sheet in sheet_range:
        df = None
        try:
            # Polars read_excel uses 0-based sheet_id by default when int is passed.
            # Our sheet_range contains 0-based indices (e.g. 2 means 3rd sheet).
            # We use infer_schema_length=0 to read as UTF8/Strings initially for safety
            # similar to pandas behavior of reading mixed types, then cast later.
            # But pl.read_excel is stricter. Let's try default inference first.
            df = pl.read_excel(path_str, sheet_id=sheet)
        except Exception:
            # Sheet might not exist or other error
            continue

        # Drop columns/rows that are all null?
        # Polars doesn't have dropna(how='all') directly for cols/rows easily in one call.
        # But usually we filter empty frames.
        if df.is_empty():
            continue

        frames.append((sheet, df))
    return frames


def _infer_year_from_path(path: Path) -> int | None:
    match = re.search(r"(19|20)\d{2}", path.name)
    if not match:
        return None
    return int(match.group(0))


def _extract_year_week(path: Path) -> tuple[int | None, int | None]:
    year_match = re.search(r"(19|20)\d{2}", path.name)
    week_match = re.search(r"(?:-)(\d{2})|zensu(\d{2})", path.name)
    year = int(year_match.group(0)) if year_match else None
    week = None
    if week_match:
        week = int(week_match.group(1) or week_match.group(2))
    return year, week


def _sheet_range_for_year(year: int) -> range:
    if year == 1999:
        return range(2, 41)
    if year in {2004, 2009, 2015}:
        return range(2, 55)
    return range(2, 54)


def _combine_confirmed_frames(
    frames: list[tuple[int, pl.DataFrame]],
    year: int,
    *,
    week_offset: int,
) -> pl.DataFrame:
    combined: list[pl.DataFrame] = []
    for sheet, df in frames:
        week = sheet + week_offset
        df = df.with_columns([pl.lit(year).alias("year"), pl.lit(week).alias("week")])
        combined.append(df)
    if not combined:
        return pl.DataFrame()

    # Use diagonal_relaxed to handle slight schema mismatches if any
    return pl.concat(combined, how="diagonal_relaxed")


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


def _iso_week_date(year: int, week: int) -> dt.date | None:
    try:
        return dt.date.fromisocalendar(int(year), int(week), 7)
    except Exception:
        return None


def _read_confirmed_pl(
    path: Path,
    *,
    type: DatasetName | None = None,
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
    if df.columns[0] != "prefecture":
        df = df.rename({df.columns[0]: "prefecture"})

    cleaned_names = _col_rename(df.columns)
    if len(cleaned_names) == len(df.columns):
        df = df.rename(dict(zip(df.columns, cleaned_names, strict=True)))
    else:
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


def _read_bullet_pl(
    path: Path,
    *,
    year: int | None = None,
    week: Iterable[int] | None = None,
) -> pl.DataFrame:
    # Handle single file or directory
    if path.is_dir():
        files = list(path.glob("*.csv"))
    else:
        files = [path]

    if week is not None:
        week_set = {int(w) for w in week}
        files = [p for p in files if (_extract_year_week(p)[1] in week_set)]

    frames: list[pl.DataFrame] = []
    for p in sorted(files):
        try:
            df = pl.read_csv(p, infer_schema_length=0)
            cleaned = _col_rename_bullet(df.columns)
            if len(cleaned) == len(df.columns):
                df = df.rename(dict(zip(df.columns, cleaned, strict=True)))
            if df.columns[0] != "prefecture":
                df = df.rename({df.columns[0]: "prefecture"})

            file_year, file_week = _extract_year_week(p)
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
        except Exception:
            continue

    if not frames:
        return pl.DataFrame()
    return pl.concat(frames, how="vertical")


def download(
    name: DatasetName,
    year: int,
    *,
    out_dir: Path | str | None = None,
    overwrite: bool = False,
    # Bullet specific options:
    week: int | Iterable[int] | None = None,
    lang: Literal["en", "ja"] = "en",
) -> Path | list[Path]:
    """
    Download raw data for a specific year.

    Args:
        name: Dataset name ("sex", "place", "bullet").
        year: Year of the data (e.g., 2023).
        out_dir: Directory to save file. Defaults to system cache.
        overwrite: If True, overwrite existing file.
        week: (Bullet only) Specific week(s) to download.
        lang: (Bullet only) Language for bullet data ("en", "ja").

    Returns:
        Path to the downloaded file (for sex/place) or list of Paths (for bullet).
    """
    config = get_config()
    if out_dir is None:
        base_cache = Path(user_cache_dir("jpinfectpy"))
        if name == "bullet":
            out_dir = base_cache / "raw" / "bullet"
        else:
            out_dir = base_cache / "raw" / "confirmed"

    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if name == "bullet":
        urls = url_bullet(year, week, lang=lang)
        if not urls:
            return []

        existing = {p.name: p for p in out_dir.glob("*.csv")}
        if overwrite:
            return download_urls(urls, out_dir, config)

        needed = [url for url in urls if Path(url).name not in existing]
        if not needed:
            return [existing[Path(url).name] for url in urls]

        downloaded = download_urls(needed, out_dir, config)
        # Return all requested (existing + downloaded)
        downloaded_map = {p.name: p for p in downloaded}
        results: list[Path] = []
        for url in urls:
            fname = Path(url).name
            if fname in existing:
                results.append(existing[fname])
            elif fname in downloaded_map:
                results.append(downloaded_map[fname])
        return results

    else:
        # Confirmed (sex or place)
        type_ = name  # type: ignore
        url = url_confirmed(year, type_)
        filename = f"{year}_{Path(url).name}"
        dest = out_dir / filename

        if dest.exists() and not overwrite:
            return dest

        downloaded = download_urls([url], out_dir, config)
        if not downloaded:
            raise RuntimeError(f"Failed to download {name} data for year {year}")

        # Rename if needed (download_urls uses basename)
        # But we want {year}_{basename}. download_urls returns list of paths.
        actual_file = downloaded[0]
        if actual_file.name != filename:
            # We want to enforce our naming convention if we downloaded to a temp name?
            # actually download_urls saves as verify_urls.py showed.
            # To respect {year}_ prefix, we might need to rename it
            # But download_urls saves with original basename.
            final_dest = out_dir / filename
            if actual_file != final_dest:
                actual_file.rename(final_dest)
                return final_dest

        return actual_file


def read(
    path: Path | str,
    type: DatasetName | None = None,
    *,
    return_type: ReturnType | None = None,
) -> AnyFrame:
    """
    Read a local raw file into a DataFrame.

    Args:
        path: Path to the Excel or CSV file (or directory).
        type: "sex", "place", or "bullet". Inferred from filename if None.
        return_type: "polars" or "pandas".
    """
    path = Path(path)
    if type is None:
        # Infer type
        if path.suffix == ".csv" or (path.is_dir() and list(path.glob("*.csv"))):
            type = "bullet"
        elif "Syu_01" in path.name or "sex" in path.name:
            type = "sex"
        elif "Syu_02" in path.name or "place" in path.name:
            type = "place"
        else:
            # Default fallback?
            raise ValueError("Could not infer dataset type from filename. Please specify 'type'.")

    if type == "bullet":
        df = _read_bullet_pl(path)
    else:
        df = _read_confirmed_pl(path, type=type)

    if resolve_return_type(return_type) == "pandas":
        return to_pandas(df)
    return df
