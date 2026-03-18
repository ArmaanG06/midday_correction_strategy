"""
Needs to out put:

equity curve -> DF with timestamp, cash, market_value, total_equity
trades log -> df with trade_id, symbol, side, entry_time, exit_time, quantity, entry_price, exit_price, pnl, return_pct
fills log -> df with timestamp, symbol, side, quantity, fill_price, commission, slippage
position history -> df with timestamp, symbol, quantity, avg_price, market_price, unrealized_pnl
cash history 
Drawdown series, or at least enough data to derive it which may be the data above


"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Dict, List
import pandas as pd
import numpy as np

SIGNAL_COLS = ['ticker', 'timestamp', 'entry', 'position', 'price', 'amount']


@dataclass
class OpenTrade:
    trade_id:int
    symbol:str
    side: str # long or short
    entry_time = pd.Timestamp
    quantity: float
    entry_price: float
    entry_commission: float
    entry_slippage: float


def _validate_signals(signals: pd.DataFrame):
    required = set(SIGNAL_COLS)
    missing = required - set(signals.columns)
    if missing:
        raise ValueError(f"signals missing required columns: {sorted(missing)}")
    
    df = signals.copy

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["ticker"] = df["ticker"].astype(str)
    df["entry"] = df["entry"].astype(float)
    df["position"] = df["position"].astype(float)
    df["price"] = df["price"].astype(float)
    df["amount"] = df["amount"].astype(float)

    df = df.sort_values(["timestamp", "ticker"]).reset_index(drop=True)

    return df

def _prepare_bars(bars: Optional[pd.DataFrame]):
    if bars is None:
        return None
    required = {"symbol", "bar_ts", "close"}
    missing = required - set(bars.columns)

    if missing:
        raise ValueError(f"signals missing required columns: {sorted(missing)}")
    
    out = bars.copy()
    out['bars_ts'] = pd.to_datetime(out['bars_ts'])
    out = out.sort_values(["bar_ts", "symbol"]).reset_index(drop=True)
    return out

def _get_fill_price(signal_price: float, entry_qty: float, slippage_bps: float):
    """
    Positive entry_qty = buy
    Negative entry_qty = sell
    Slippage worsens the fill in the direction of the trade.
    """
    slip_fraction = slippage_bps / 10_000.0
    if entry_qty > 0:
        fill_price = signal_price * (1 + slip_fraction)
    else:
        fill_price = signal_price * (1 - slip_fraction)

    slippage_per_share = abs(fill_price - signal_price)
    return fill_price, slippage_per_share




def backtesting_engine(raw: pd.DataFrame, bars: Optional[pd.DataFrame] = None, initial_cash: float = 100_000.0, commission_per_share: float = 0.0, slippage_bps: float = 0.0):
    """
    Basic event-driven backtester.

    Parameters
    ----------
    raw : pd.DataFrame
        Signal dataframe with columns:
        [ticker, timestamp, entry, position, price, amount]

        Conventions:
        - entry is signed direction (+1 buy, -1 sell)
        - amount is absolute number of units per signal
        - signed quantity executed = entry * amount

    bars : Optional[pd.DataFrame]
        Optional market bars for mark-to-market valuation.
        Expected columns at minimum:
        [symbol, bar_ts, close]

    Returns
    -------
    equity_curve : pd.DataFrame
        [timestamp, cash, market_value, total_equity]

    trade_log : pd.DataFrame
        [trade_id, symbol, side, entry_time, exit_time, quantity,
         entry_price, exit_price, pnl, return_pct]

    fills_log : pd.DataFrame
        [timestamp, symbol, side, quantity, fill_price, commission, slippage]

    position_history : pd.DataFrame
        [timestamp, symbol, quantity, avg_price, market_price, unrealized_pnl]

    cash_history : pd.DataFrame
        [timestamp, cash]

    drawdown_series : pd.DataFrame
        [timestamp, total_equity, running_peak, drawdown, drawdown_pct]
    """

    signals = _validate_signals(raw)
    bars = _prepare_bars(bars)

    fills_log: List[Dict] = []
    trade_log: List[Dict] = []
    equity_curve: List[Dict] = []
    position_history: List[Dict] = []
    cash_history: List[Dict] = []

    cash = float(initial_cash)
    next_trade_id = 1


    return equity_curve, trade_log, fills_log, position_history, cash_history


"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Dict, List
import pandas as pd
import numpy as np


SIGNAL_COLUMNS = ["ticker", "timestamp", "entry", "position", "price", "amount"]


@dataclass
class OpenTrade:
    trade_id: int
    symbol: str
    side: str          # "long" or "short"
    entry_time: pd.Timestamp
    quantity: float
    entry_price: float
    entry_commission: float
    entry_slippage: float


def _validate_signals(signals: pd.DataFrame) -> pd.DataFrame:
    required = set(SIGNAL_COLUMNS)
    missing = required - set(signals.columns)
    if missing:
        raise ValueError(f"signals missing required columns: {sorted(missing)}")

    df = signals.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["ticker"] = df["ticker"].astype(str)
    df["entry"] = df["entry"].astype(float)
    df["position"] = df["position"].astype(float)
    df["price"] = df["price"].astype(float)
    df["amount"] = df["amount"].astype(float)

    df = df.sort_values(["timestamp", "ticker"]).reset_index(drop=True)
    return df


def _prepare_bars(bars: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
    if bars is None:
        return None

    required = {"symbol", "bar_ts", "close"}
    missing = required - set(bars.columns)
    if missing:
        raise ValueError(f"bars missing required columns: {sorted(missing)}")

    out = bars.copy()
    out["bar_ts"] = pd.to_datetime(out["bar_ts"])
    out = out.sort_values(["bar_ts", "symbol"]).reset_index(drop=True)
    return out


def _get_fill_price(signal_price: float, entry_qty: float, slippage_bps: float) -> tuple[float, float]:
    ""
    Positive entry_qty = buy
    Negative entry_qty = sell
    Slippage worsens the fill in the direction of the trade.
    ""
    slip_fraction = slippage_bps / 10_000.0
    if entry_qty > 0:
        fill_price = signal_price * (1 + slip_fraction)
    else:
        fill_price = signal_price * (1 - slip_fraction)

    slippage_per_share = abs(fill_price - signal_price)
    return fill_price, slippage_per_share


def backtesting_engine(
    raw: pd.DataFrame,
    bars: Optional[pd.DataFrame] = None,
    initial_cash: float = 100_000.0,
    commission_per_share: float = 0.0,
    slippage_bps: float = 0.0,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    ""
    Basic event-driven backtester.

    Parameters
    ----------
    raw : pd.DataFrame
        Signal dataframe with columns:
        [ticker, timestamp, entry, position, price, amount]

        Conventions:
        - entry is signed direction (+1 buy, -1 sell)
        - amount is absolute number of units per signal
        - signed quantity executed = entry * amount

    bars : Optional[pd.DataFrame]
        Optional market bars for mark-to-market valuation.
        Expected columns at minimum:
        [symbol, bar_ts, close]

    Returns
    -------
    equity_curve : pd.DataFrame
        [timestamp, cash, market_value, total_equity]

    trade_log : pd.DataFrame
        [trade_id, symbol, side, entry_time, exit_time, quantity,
         entry_price, exit_price, pnl, return_pct]

    fills_log : pd.DataFrame
        [timestamp, symbol, side, quantity, fill_price, commission, slippage]

    position_history : pd.DataFrame
        [timestamp, symbol, quantity, avg_price, market_price, unrealized_pnl]

    cash_history : pd.DataFrame
        [timestamp, cash]

    drawdown_series : pd.DataFrame
        [timestamp, total_equity, running_peak, drawdown, drawdown_pct]
    ""
    signals = _validate_signals(raw)
    bars = _prepare_bars(bars)

    fills_log: List[Dict] = []
    trade_log: List[Dict] = []
    equity_curve: List[Dict] = []
    position_history: List[Dict] = []
    cash_history: List[Dict] = []

    cash = float(initial_cash)
    next_trade_id = 1

    # Per-symbol position state
    positions: Dict[str, Dict] = {}
    # Per-symbol currently open trade metadata
    open_trades: Dict[str, OpenTrade] = {}

    # Build event timeline
    signal_times = set(signals["timestamp"].tolist())
    bar_times = set(bars["bar_ts"].tolist()) if bars is not None else set()
    event_times = sorted(signal_times | bar_times)

    signals_by_time = {
        ts: df for ts, df in signals.groupby("timestamp", sort=True)
    }

    if bars is not None:
        bars_by_time = {
            ts: df for ts, df in bars.groupby("bar_ts", sort=True)
        }
    else:
        bars_by_time = {}

    # Last known prices for MTM
    last_price: Dict[str, float] = {}

    def ensure_position(symbol: str) -> None:
        if symbol not in positions:
            positions[symbol] = {
                "quantity": 0.0,
                "avg_price": 0.0,
            }

    def mark_to_market(ts: pd.Timestamp) -> None:
        market_value = 0.0

        all_symbols = set(positions.keys()) | set(last_price.keys())
        for symbol in sorted(all_symbols):
            ensure_position(symbol)
            qty = positions[symbol]["quantity"]
            avg_px = positions[symbol]["avg_price"]
            mkt_px = last_price.get(symbol, avg_px if avg_px != 0 else np.nan)

            if not pd.isna(mkt_px):
                market_value += qty * mkt_px
                unrealized = qty * (mkt_px - avg_px)
            else:
                unrealized = np.nan

            if qty != 0 or symbol in last_price:
                position_history.append(
                    {
                        "timestamp": ts,
                        "symbol": symbol,
                        "quantity": qty,
                        "avg_price": avg_px,
                        "market_price": mkt_px,
                        "unrealized_pnl": unrealized,
                    }
                )

        total_equity = cash + market_value

        equity_curve.append(
            {
                "timestamp": ts,
                "cash": cash,
                "market_value": market_value,
                "total_equity": total_equity,
            }
        )

        cash_history.append(
            {
                "timestamp": ts,
                "cash": cash,
            }
        )

    for ts in event_times:
        # 1) update last prices from bars first
        if ts in bars_by_time:
            for _, row in bars_by_time[ts].iterrows():
                last_price[row["symbol"]] = float(row["close"])

        # 2) process signals at this timestamp
        if ts in signals_by_time:
            current_signals = signals_by_time[ts].sort_values("ticker")

            for _, sig in current_signals.iterrows():
                symbol = sig["ticker"]
                signal_price = float(sig["price"])
                signed_qty = float(sig["entry"]) * float(sig["amount"])

                if signed_qty == 0:
                    continue

                ensure_position(symbol)

                side = "buy" if signed_qty > 0 else "sell"
                fill_price, per_share_slippage = _get_fill_price(
                    signal_price=signal_price,
                    entry_qty=signed_qty,
                    slippage_bps=slippage_bps,
                )

                quantity_abs = abs(signed_qty)
                commission = commission_per_share * quantity_abs
                total_slippage = per_share_slippage * quantity_abs

                # Cash impact
                # buy -> cash decreases
                # sell -> cash increases
                cash -= signed_qty * fill_price
                cash -= commission

                fills_log.append(
                    {
                        "timestamp": ts,
                        "symbol": symbol,
                        "side": side,
                        "quantity": quantity_abs,
                        "fill_price": fill_price,
                        "commission": commission,
                        "slippage": total_slippage,
                    }
                )

                pos_qty = positions[symbol]["quantity"]
                pos_avg = positions[symbol]["avg_price"]
                new_qty = pos_qty + signed_qty

                # Case A: opening from flat
                if pos_qty == 0:
                    positions[symbol]["quantity"] = new_qty
                    positions[symbol]["avg_price"] = fill_price

                    open_trades[symbol] = OpenTrade(
                        trade_id=next_trade_id,
                        symbol=symbol,
                        side="long" if signed_qty > 0 else "short",
                        entry_time=ts,
                        quantity=quantity_abs,
                        entry_price=fill_price,
                        entry_commission=commission,
                        entry_slippage=total_slippage,
                    )
                    next_trade_id += 1

                # Case B: adding same direction
                elif np.sign(pos_qty) == np.sign(signed_qty):
                    total_cost = (abs(pos_qty) * pos_avg) + (quantity_abs * fill_price)
                    total_qty = abs(pos_qty) + quantity_abs
                    positions[symbol]["quantity"] = new_qty
                    positions[symbol]["avg_price"] = total_cost / total_qty

                    # For this basic engine, keep original open trade and just increase quantity
                    if symbol in open_trades:
                        open_trades[symbol].quantity += quantity_abs

                # Case C: reducing or closing existing position
                else:
                    closing_qty = min(abs(pos_qty), quantity_abs)

                    if symbol not in open_trades:
                        raise RuntimeError(f"Missing open trade state for {symbol}")

                    ot = open_trades[symbol]

                    if pos_qty > 0:
                        # closing long by selling
                        realized_pnl = closing_qty * (fill_price - pos_avg)
                    else:
                        # closing short by buying
                        realized_pnl = closing_qty * (pos_avg - fill_price)

                    # If fully closed, write trade log
                    fully_closed = abs(new_qty) < 1e-12
                    flipped = np.sign(pos_qty) != np.sign(new_qty) and abs(new_qty) > 1e-12

                    exit_commission_alloc = commission if fully_closed or flipped else commission
                    gross_entry_value = ot.quantity * ot.entry_price
                    net_pnl = realized_pnl - ot.entry_commission - exit_commission_alloc

                    if fully_closed or flipped:
                        trade_log.append(
                            {
                                "trade_id": ot.trade_id,
                                "symbol": symbol,
                                "side": ot.side,
                                "entry_time": ot.entry_time,
                                "exit_time": ts,
                                "quantity": closing_qty,
                                "entry_price": ot.entry_price,
                                "exit_price": fill_price,
                                "pnl": net_pnl,
                                "return_pct": (net_pnl / gross_entry_value) if gross_entry_value != 0 else np.nan,
                            }
                        )

                    if fully_closed:
                        positions[symbol]["quantity"] = 0.0
                        positions[symbol]["avg_price"] = 0.0
                        del open_trades[symbol]

                    elif flipped:
                        # old trade closed, new trade opened in opposite direction
                        positions[symbol]["quantity"] = new_qty
                        positions[symbol]["avg_price"] = fill_price

                        del open_trades[symbol]
                        open_trades[symbol] = OpenTrade(
                            trade_id=next_trade_id,
                            symbol=symbol,
                            side="long" if new_qty > 0 else "short",
                            entry_time=ts,
                            quantity=abs(new_qty),
                            entry_price=fill_price,
                            entry_commission=commission,
                            entry_slippage=total_slippage,
                        )
                        next_trade_id += 1

                    else:
                        # partial reduction
                        positions[symbol]["quantity"] = new_qty
                        # avg price unchanged on partial close for remaining position
                        positions[symbol]["avg_price"] = pos_avg

                        # reduce open trade quantity
                        open_trades[symbol].quantity -= closing_qty

                last_price[symbol] = fill_price

        # 3) snapshot after processing all events at ts
        mark_to_market(ts)

    equity_df = pd.DataFrame(equity_curve).sort_values("timestamp").reset_index(drop=True)
    trade_df = pd.DataFrame(trade_log)
    fills_df = pd.DataFrame(fills_log).sort_values(["timestamp", "symbol"]).reset_index(drop=True)
    pos_hist_df = pd.DataFrame(position_history).sort_values(["timestamp", "symbol"]).reset_index(drop=True)
    cash_df = pd.DataFrame(cash_history).sort_values("timestamp").reset_index(drop=True)

    if equity_df.empty:
        drawdown_df = pd.DataFrame(
            columns=["timestamp", "total_equity", "running_peak", "drawdown", "drawdown_pct"]
        )
    else:
        drawdown_df = equity_df[["timestamp", "total_equity"]].copy()
        drawdown_df["running_peak"] = drawdown_df["total_equity"].cummax()
        drawdown_df["drawdown"] = drawdown_df["total_equity"] - drawdown_df["running_peak"]
        drawdown_df["drawdown_pct"] = np.where(
            drawdown_df["running_peak"] != 0,
            drawdown_df["drawdown"] / drawdown_df["running_peak"],
            np.nan,
        )

    if trade_df.empty:
        trade_df = pd.DataFrame(
            columns=[
                "trade_id", "symbol", "side", "entry_time", "exit_time",
                "quantity", "entry_price", "exit_price", "pnl", "return_pct"
            ]
        )
    else:
        trade_df = trade_df.sort_values(["entry_time", "symbol"]).reset_index(drop=True)

    return equity_df, trade_df, fills_df, pos_hist_df, cash_df, drawdown_df
"""