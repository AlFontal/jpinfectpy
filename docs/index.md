# jpinfectpy

Python port of the R package `jpinfect` for Japanese infectious disease surveillance data.

## Quickstart

```python
from pathlib import Path
import jpinfectpy as jp

jp.configure(return_type="pandas", rate_limit_per_minute=10)

sex_path = jp.get_confirmed(2022, "sex", Path("data/raw"))
confirmed = jp.read_confirmed(sex_path, type="sex")

bullet = jp.read_bullet(Path("data/raw"), year=2025, week=[1, 2], lang="en")
wide = jp.pivot(bullet)
```

## Key Points

- Polars is used internally for all transformations.
- Pandas output is the default for public functions.
- Disk caching and rate limiting are enabled by default.
