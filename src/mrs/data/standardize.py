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
    
    if df is None or len(df) == 0:
        return pd.DataFrame(columns=BARS_COLS)
    
    out = df.copy()

    # Timestamp col normalization

    if ts_col not in out.columns:
        #common fallbacks
        for alt in ["datetime", "date", "timestamp", "time"]:
            if alt in out.columns:
                out = out.rename(columns={alt: ts_col})
                break
    
    if ts_col not in out.columns:
        raise ValueError(f"Failed timestamp column. Expected '{ts_col}' or one of datetime/date/timestamp/time")
    
    out[ts_col] = pd.to_datetime(out[ts_col], errors="coerce")
    if out[ts_col].isna().any():
        pass

    # Symbol Normalization

    # Price/vol typing

    # Final Canonical naming + ordering

    # de-dupe + sort (project-wide expectation)

    # canonical column order only (drop extra)
    return out.reindex(columns=BARS_COLS)

def assert_bars_contract(df: pd.DataFrame):
    """
    Raises if df violates the canonical vars contract
    Use in tests and at boundries (after loading and fetching)
    """
    pass

