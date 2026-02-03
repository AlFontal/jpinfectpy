from __future__ import annotations

from typing import Literal, TypeAlias

import pandas as pd
import polars as pl

ReturnType = Literal["pandas", "polars"]
DatasetName = Literal["sex", "place", "bullet"]
AnyFrame: TypeAlias = pd.DataFrame | pl.DataFrame
