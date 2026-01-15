from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional
import hashlib

@dataclass(frozen=True)
class BarsRequest:
    """
    Cache identiy for a bars request

    Choose either:
        - (start, end) OR
        - (duration, end_datetime)

    for v1, if you only use duration/end_dateimte, keep start/end = None
    """

def cache_path(root: str | Path, req: BarsRequest) -> Path:
    """
    Deterministic parquet path for a request.

    Layout is designed to be:
      - human-browsable (vendor/bar_size/symbol)
      - collision-proof (fingerprint)
    """