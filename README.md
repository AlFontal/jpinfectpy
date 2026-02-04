# jpinfectpy

Python port of the R package `jpinfect` for Japanese infectious disease surveillance data. This is an independent reimplementation that mirrors the public API and behavior where practical.

Upstream R package: [TomonoriHoshi/jpinfect](https://github.com/TomonoriHoshi/jpinfect)

# Load combined historical + recent data (updates automatically)
df_all = jp.load_all()

# Download current weekly reports (2024+)
files = jp.download_recent()
```

Data source: [Infectious Disease Surveillance (JIHS)](https://id-info.jihs.go.jp/)

For detailed descriptions of the available datasets, see [DATASETS.md](./DATASETS.md).

## Install

```bash
uv pip install jpinfectpy
```

## Quickstart

```python
import jpinfectpy as jp

# 1. Load bundled High-Quality Data (Recommended)
# Includes historical data (sex: 1999-2023, place: 2001-2023)
df_sex = jp.load("sex")
df_place = jp.load("place")

# 2. Download & Read Latest Raw Data
# Download to system cache and read
path = jp.download("sex", 2024)
df_2024 = jp.read(path)

# 3. Analyze Patterns (Bullet/Weekly Reports)
# Download specific weeks
bullet_paths = jp.download("bullet", 2025, week=[1, 2])
df_bullet = jp.read(bullet_paths[0])
```

## Return Types

All transformations are implemented in Polars. Each public data-returning function accepts `return_type="pandas"|"polars"`. When omitted, it uses the global configuration.

## Deviations from Upstream

- Excel parsing is a best-effort adaptation of the upstream logic; if the official workbook layout changes, you may need to adjust parsing or pre-clean inputs.
- URL logic matches upstream, but includes sanity checks and clearer exceptions.

## Polite Use

Please do not download data excessively. This library includes a disk cache, rate limiting, and a clear user-agent by default. For bulk downloads, increase the rate limit and cache directory thoughtfully and consider running during off-peak hours.

## R-to-Python API Map

| R function | Python function | Notes |
| --- | --- | --- |
| `jpinfect_url_confirmed()` | `url_confirmed()` | Same URL logic |
| `jpinfect_url_bullet()` | `url_bullet()` | Same URL logic |
| `data()` | `load()` | Bundled datasets |
| `jpinfect_get_confirmed()` | `download()` | Unified download |
| `jpinfect_get_bullet()` | `download()` | Unified download |
| `jpinfect_read_confirmed()` | `read()` | Unified read |
| `jpinfect_read_bullet()` | `read()` | Unified read |
| `jpinfect_merge()` | `merge()` | Full join + bind rows |
| `jpinfect_pivot()` | `pivot()` | Wide/long conversion |

## License and Attribution

This project is licensed under GPL-3.0-or-later and includes attribution to the upstream R package. See `LICENSE` and `CITATION.cff`.
