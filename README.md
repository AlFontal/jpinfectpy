# jpinfectpy

Python port of the R package `jpinfect` for Japanese infectious disease surveillance data. This is an independent reimplementation that mirrors the public API and behavior where practical.

Upstream R package: [TomonoriHoshi/jpinfect](https://github.com/TomonoriHoshi/jpinfect)

Data source: [Infectious Disease Surveillance (JIHS)](https://id-info.jihs.go.jp/)

## Install

```bash
uv pip install jpinfectpy
```

## Quickstart

```python
from pathlib import Path
import jpinfectpy as jp

# Configure defaults
jp.configure(return_type="pandas", rate_limit_per_minute=10)

# Build URLs
jp.url_confirmed(2022, "sex")
jp.url_bullet(2025, week=1, lang="en")

# Download raw files
out_dir = Path("data/raw")
sex_path = jp.get_confirmed(2022, "sex", out_dir)
bullet_paths = jp.get_bullet(2025, week=[1, 2], out_dir=out_dir, lang="en")

# Read data (Polars internal, Pandas output by default)
confirmed = jp.read_confirmed(sex_path, type="sex")
bullet = jp.read_bullet(out_dir, year=2025, week=[1, 2], lang="en")

# Merge and pivot
merged = jp.merge(confirmed, jp.read_confirmed(sex_path, type="place"))
wide = jp.pivot(bullet)
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
| `jpinfect_get_confirmed()` | `get_confirmed()` | Downloads raw Excel |
| `jpinfect_get_bullet()` | `get_bullet()` | Downloads raw CSV |
| `jpinfect_read_confirmed()` | `read_confirmed()` | Polars-first parsing |
| `jpinfect_read_bullet()` | `read_bullet()` | Polars-first parsing |
| `jpinfect_merge()` | `merge()` | Full join + bind rows |
| `jpinfect_pivot()` | `pivot()` | Wide/long conversion |

## License and Attribution

This project is licensed under GPL-3.0-or-later and includes attribution to the upstream R package. See `LICENSE` and `CITATION.cff`.
