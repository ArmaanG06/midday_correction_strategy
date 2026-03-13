from dataclasses import dataclass
from src.mrs.data.main_load import load_id_intraday_data  # soon to be added
import pandas as pd

COLUMN_NAMES = ["ticker", "timestamp", "entry", "position", "price", "amount"]

params_1m = {
    "bar_size": 1,
    "price_inc": 0.05,
    "vol_inc": 0.05,
    "entry_time_1": 1,   # hours into session
    "exit_time_1": 3,
}

params_5m = {
    "bar_size": 5,
    "price_inc": 0.05,
    "vol_inc": 0.05,
    "entry_time_1": 1.5,  # hours into session
    "exit_time_1": 3,
}

params_15m = {
    "bar_size": 15,
    "price_inc": 0.05,
    "vol_inc": 0.05,
    "entry_time_1": 0.5,  # hours into session
    "exit_time_1": 4,
    "entry_time_2": 3,
    "exit_time_2": 6,
}


def _build_signal_row(
    ticker: str,
    timestamp: pd.Timestamp,
    entry: int,
    position: int,
    price: float,
    amount: float = 1.0,
) -> dict:
    return {
        "ticker": ticker,
        "timestamp": timestamp,
        "entry": int(entry),
        "position": int(position),
        "price": float(price),
        "amount": float(amount),
    }


def generate_signals_from_df(bars: pd.DataFrame, params, param_version: str = "1") -> pd.DataFrame:
    """
    Generates raw event-driven signals from intraday bars.

    Mean reversion logic:
    - If price has moved up enough from the day's opening bar and volume has expanded enough,
      short at entry_time.
    - If price has moved down enough from the day's opening bar and volume has expanded enough,
      long at entry_time.
    - Exit at exit_time.

    Expected columns in bars:
    bar_id, symbol, bar_ts, bar_size, open, high, low, close, volume, source, created_at
    """
    required_cols = {"symbol", "bar_ts", "open", "close", "volume"}
    missing = required_cols - set(bars.columns)
    if missing:
        raise ValueError(f"bars is missing required columns: {sorted(missing)}")

    bars = bars.copy()
    bars["bar_ts"] = pd.to_datetime(bars["bar_ts"])
    bars = bars.sort_values(["symbol", "bar_ts"]).reset_index(drop=True)

    bar_size = params["bar_size"]
    bars_per_hour = int(60 / bar_size)

    entry_idx = int(params[f"entry_time_{param_version}"] * bars_per_hour)
    exit_idx = int(params[f"exit_time_{param_version}"] * bars_per_hour)

    signal_rows = []

    # group by symbol and trading day
    for (ticker, day), day_df in bars.groupby(["symbol", bars["bar_ts"].dt.date], sort=True):
        day_df = day_df.sort_values("bar_ts").reset_index(drop=True)

        # Need enough bars to both enter and exit
        if len(day_df) <= max(entry_idx, exit_idx):
            continue

        open_bar = day_df.iloc[0]
        entry_bar = day_df.iloc[entry_idx]
        exit_bar = day_df.iloc[exit_idx]

        open_price = open_bar["open"]
        open_volume = open_bar["volume"]

        if pd.isna(open_price) or open_price == 0:
            continue
        if pd.isna(open_volume) or open_volume == 0:
            continue

        price_change = (entry_bar["open"] - open_price) / open_price
        vol_change = (entry_bar["volume"] - open_volume) / open_volume

        direction = 0

        # Mean reversion:
        # big up move -> short
        if price_change >= params["price_inc"] and vol_change >= params["vol_inc"]:
            direction = -1

        # big down move -> long
        elif price_change <= -params["price_inc"] and vol_change >= params["vol_inc"]:
            direction = 1

        if direction == 0:
            continue

        # Entry signal
        signal_rows.append(
            _build_signal_row(
                ticker=ticker,
                timestamp=entry_bar["bar_ts"],
                entry=direction,
                position=direction,
                price=entry_bar["open"],
                amount=1.0,
            )
        )

        # Exit signal: reverse current position so net goes back to flat
        signal_rows.append(
            _build_signal_row(
                ticker=ticker,
                timestamp=exit_bar["bar_ts"],
                entry=-direction,
                position=0,
                price=exit_bar["open"],
                amount=1.0,
            )
        )

    signals_df = pd.DataFrame(signal_rows, columns=COLUMN_NAMES)
    if not signals_df.empty:
        signals_df = signals_df.sort_values(["ticker", "timestamp"]).reset_index(drop=True)

    return signals_df


def clean_signals(signals_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans raw signals by:
    - sorting
    - removing exact duplicate timestamps per ticker by summing entry
    - recomputing position from entry
    - dropping zero-entry rows created by netting
    """
    if signals_raw.empty:
        return pd.DataFrame(columns=COLUMN_NAMES)

    df = signals_raw.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    df = (
        df.groupby(["ticker", "timestamp", "price"], as_index=False)
        .agg({"entry": "sum", "amount": "sum"})
        .sort_values(["ticker", "timestamp"])
        .reset_index(drop=True)
    )

    # Remove rows that net to zero
    df = df[df["entry"] != 0].copy()

    # Recompute running position per ticker
    df["position"] = df.groupby("ticker")["entry"].cumsum()

    # Optional safety clamp in case bad inputs produce larger absolute positions
    df["position"] = df["position"].clip(-1, 1)

    return df[COLUMN_NAMES].reset_index(drop=True)


def generate_signals(symbol: str, duration: str, bar_size: str, params) -> pd.DataFrame:
    bars = loader.load(symbol, duration, bar_size)
    signals_raw = generate_signals_from_df(bars, params, "1")
    signals_pure = clean_signals(signals_raw)
    return signals_raw, signals_pure