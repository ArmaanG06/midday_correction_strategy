from __future__ import annotations
import pandas as pd

def standardize_bars(
    raw_df: pd.DataFrame,
    ticker: str,
    bar_size: str,
    source: str,
) -> pd.DataFrame:
    """
    Convert raw IB bars into the schema expected by market_data.intraday_bars.
    """
    df = raw_df.copy()

    required = ["datetime", "open", "high", "low", "close", "volume"]
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    df["symbol"] = ticker

    # IB historical bars usually come back as strings like '20260310 14:35:00'
    df["bar_ts"] = pd.to_datetime(df["datetime"], utc=True, errors="coerce")

    if df["bar_ts"].isna().any():
        bad_rows = df[df["bar_ts"].isna()]
        raise ValueError(f"Failed to parse bar timestamps for rows: {bad_rows.to_dict(orient='records')}")

    df["bar_size"] = bar_size
    df["source"] = source

    if "vwap" not in df.columns:
        df["vwap"] = None

    if "trade_count" not in df.columns:
        df["trade_count"] = None

    final_columns = [
        "symbol",
        "bar_ts",
        "bar_size",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "vwap",
        "trade_count",
        "source",
    ]

    return df[final_columns].sort_values(["symbol", "bar_ts"]).reset_index(drop=True)