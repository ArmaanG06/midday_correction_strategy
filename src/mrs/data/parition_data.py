import pandas as pd
from typing import Dict, Tuple

def partition_data(df, ticker, bar_interval):
    """
    Partition standardized OHLCV dataframe into day buckets.

    Returns
    -------
    Dict[(ticker, bar_interval, year, month, day) -> partition_df]
    where each partition_df is sorted by datetime and contains:
      ['datetime','open','high','low','close','volume','ticker','bar_interval','year','month','day']
    """

    required_cols = {'datetime', 'open', 'high', 'low', 'close', 'volume'}
    missing = required_cols - set(df.columns)

    if missing:
        raise ValueError(f"Missing Columns: {missing}")
    if not ticker or not isinstance(ticker, str):
        raise ValueError(f"Ticker must be a non-empty string")
    if not bar_interval or not isinstance(bar_interval, str):
        raise ValueError("bar_interval must be a non-empty string")
    
    out = df.copy()

    out["datetime"] = pd.to_datetime(out["datetime"], utc=True)
    out["bar_interval"] = bar_interval
    out["year"] = out["datetime"].dt.year.astype("int32")
    out["month"] = out["datetime"].dt.month.astype("int8")
    out["day"] = out["datetime"].dt.day.astype("int8")

    paritions: Dict[Tuple[str, str, int, int, int], pd.DataFrame] = {}
    for (y,m,d),g in out.groupby(["year", "month", "day"], sort=True):
        key = (ticker, bar_interval, int(y), int(m), int(d))
        paritions[key] = (
            g.sort_values("datetime").reset_index(drop=True)
        )

    return paritions
