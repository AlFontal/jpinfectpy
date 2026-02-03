# R to Python Mapping

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
