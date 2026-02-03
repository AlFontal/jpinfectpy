from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path

from platformdirs import user_cache_dir

from .types import ReturnType


@dataclass(frozen=True)
class Config:
    return_type: ReturnType = "pandas"
    cache_dir: Path = Path(user_cache_dir("jpinfectpy"))
    rate_limit_per_minute: int = 20
    user_agent: str = "jpinfectpy/0.1.0 (+https://github.com/your-org/jpinfectpy)"
    timeout_seconds: float = 30.0
    retries: int = 3


_CONFIG = Config()


def get_config() -> Config:
    return _CONFIG


def configure(**kwargs: object) -> Config:
    global _CONFIG
    _CONFIG = replace(_CONFIG, **kwargs)  # type: ignore[arg-type]
    return _CONFIG
