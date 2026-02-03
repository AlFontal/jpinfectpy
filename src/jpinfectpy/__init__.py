from __future__ import annotations

from .config import Config, configure, get_config
from .datasets import load_dataset
from .io_bullet import get_bullet, read_bullet, read_bullet_pl
from .io_confirmed import get_confirmed, read_confirmed, read_confirmed_pl
from .transform import merge, pivot
from .types import AnyFrame, ReturnType
from .urls import url_bullet, url_confirmed
from .utils import to_pandas, to_polars

__all__ = [
    "AnyFrame",
    "Config",
    "ReturnType",
    "configure",
    "get_bullet",
    "get_config",
    "get_confirmed",
    "load_dataset",
    "merge",
    "pivot",
    "read_bullet",
    "read_bullet_pl",
    "read_confirmed",
    "read_confirmed_pl",
    "to_pandas",
    "to_polars",
    "url_bullet",
    "url_confirmed",
]

__version__ = "0.1.0"
