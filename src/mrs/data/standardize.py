from __future__ import annotations

from typing import Optional
import pandas as pd

BARS_COLS = ["ts", "symbol", "open", "high", "low", "close", "volume"]

def standardize_bars(df: pd.DataFrame, symbol: str, tz: str = "American/New_York", ts_col: str="ts"):
    """
    Enforce canonical bars schema
        ts (time-zone aware), symbol, OHLC (float64), volume (int64)

    Accepts common variations
        - timestamp column named 'datetime' or 'date' etc. (controlled via ts_col + fallbacks)
        - symbol coloumn may or may not exist 
    """
    # Timestamp col normalization
    

    # Symbol Normalization

    # Price/vol typing

    # Final Canonical naming + ordering

    # de-dupe + sort (project-wide expectation)

    # canonical column order only (drop extra)

def assert_bars_contract(df: pd.DataFrame):
    """
    Raises if df violates the canonical vars contract
    Use in tests and at boundries (after loading and fetching)
    """
    pass

