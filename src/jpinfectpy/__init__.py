from __future__ import annotations

from .config import Config, configure, get_config
from .datasets import load_dataset as load
from .io import DatasetName, download, read
from .transform import merge, pivot
from .types import AnyFrame, ReturnType
from .utils import to_pandas, to_polars

__all__ = [
    "AnyFrame",
    "Config",
    "DatasetName",
    "ReturnType",
    "configure",
    "download",
    "get_config",
    "load",
    "merge",
    "pivot",
    "read",
    "to_pandas",
    "to_polars",
]

__version__ = "0.1.0"
