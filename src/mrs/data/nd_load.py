from __future__ import annotations

import threading
import time
from datetime import datetime, timezone
from typing import Iterable, Optional

import pandas as pd
from ibapi.client import EClient
from ibapi.contract import Contract
from ibapi.wrapper import EWrapper

from src.mrs.data.stdize import standardize_bars


class IBFetcher(EWrapper, EClient):
    """
    Fetches historical bar data from Interactive Brokers and returns
    standardized pandas DataFrames that can be written to Supabase
    by postgres_writer.py.

    This class is intentionally responsible only for:
    1. connecting to IB
    2. requesting historical data
    3. collecting raw bars
    4. returning a cleaned DataFrame

    It does NOT write to DuckDB, Parquet, or Supabase directly.
    """

    def __init__(self) -> None:
        EClient.__init__(self, self)

        self._bars: list[dict] = []
        self._finished = threading.Event()
        self._error: Optional[str] = None
        self._req_id = 1
        self._thread: Optional[threading.Thread] = None

    def connect_app(self, host: str = "127.0.0.1", port: int = 7496, client_id: int = 1) -> None:
        print("Connecting to IB...")
        self.connect(host, port, client_id)

        self._thread = threading.Thread(target=self.run, daemon=True)
        self._thread.start()

        time.sleep(1)
        print("Connected to IB.")

    def disconnect_app(self) -> None:
        if self.isConnected():
            self.disconnect()
        print("Disconnected from IB.")

    def error(self, reqId: int, errorCode: int, errorString: str, advancedOrderRejectJson: str = "") -> None:
        message = f"IB error | reqId={reqId} code={errorCode} message={errorString}"
        print(message)

        # mark fetch as failed for serious request-level errors
        if errorCode not in {2104, 2106, 2158}:
            self._error = message
            self._finished.set()

    def historicalData(self, reqId, bar) -> None:
        """
        Called by IB for each historical bar returned.
        """
        self._bars.append(
            {
                "datetime": bar.date,
                "open": float(bar.open),
                "high": float(bar.high),
                "low": float(bar.low),
                "close": float(bar.close),
                "volume": int(bar.volume) if bar.volume is not None else 0,
            }
        )

    def historicalDataEnd(self, reqId: int, start: str, end: str) -> None:
        print(f"Historical data collection finished for request {reqId}")
        self._finished.set()

    @staticmethod
    def _build_stock_contract(
        ticker: str,
        exchange: str = "SMART",
        primary_exchange: Optional[str] = None,
        currency: str = "USD",
        sec_type: str = "STK",
    ) -> Contract:
        contract = Contract()
        contract.symbol = ticker
        contract.secType = sec_type
        contract.exchange = exchange
        contract.currency = currency

        if primary_exchange:
            contract.primaryExchange = primary_exchange

        return contract

    @staticmethod
    def _normalize_ticker(ticker: str) -> str:
        """
        Normalize symbols for internal consistency.
        Example: BRK.B -> BRK-B
        """
        return ticker.replace(".", "-").upper().strip()

    def fetch_stock_data(
        self,
        ticker: str,
        duration: str = "2 D",
        bar_size: str = "5 mins",
        exchange: str = "SMART",
        primary_exchange: Optional[str] = None,
        sec_type: str = "STK",
        currency: str = "USD",
        end_datetime: str = "",
        use_rth: int = 1,
        timeout_seconds: int = 30,
        source: str = "interactive_brokers",
    ) -> pd.DataFrame:
        """
        Fetch historical bars for a single ticker from IB and return
        a standardized DataFrame ready for postgres_writer.upsert_intraday_bars().
        """
        if not self.isConnected():
            raise RuntimeError("IB client is not connected. Call connect_app() first.")

        normalized_ticker = self._normalize_ticker(ticker)

        self._bars = []
        self._error = None
        self._finished.clear()

        contract = self._build_stock_contract(
            ticker=normalized_ticker,
            exchange=exchange,
            primary_exchange=primary_exchange,
            currency=currency,
            sec_type=sec_type,
        )

        current_req_id = self._req_id
        self._req_id += 1

        print(f"Requesting historical data for {normalized_ticker}...")

        self.reqHistoricalData(
            reqId=current_req_id,
            contract=contract,
            endDateTime=end_datetime,
            durationStr=duration,
            barSizeSetting=bar_size,
            whatToShow="TRADES",
            useRTH=use_rth,
            formatDate=1,
            keepUpToDate=False,
            chartOptions=[],
        )

        if not self._finished.wait(timeout=timeout_seconds):
            raise TimeoutError(f"Timeout fetching data for {normalized_ticker}")

        if self._error:
            raise RuntimeError(self._error)

        if not self._bars:
            print(f"No bars returned for {normalized_ticker}")
            return pd.DataFrame(
                columns=[
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
            )

        raw_df = pd.DataFrame(self._bars)

        standardized_df = standardize_bars(
            raw_df=raw_df,
            ticker=normalized_ticker,
            bar_size=bar_size,
            source=source,
        )

        return standardized_df

    def fetch_multiple_stock_data(
        self,
        tickers: Iterable[str],
        duration: str = "2 D",
        bar_size: str = "5 mins",
        exchange: str = "SMART",
        primary_exchange: Optional[str] = None,
        sec_type: str = "STK",
        currency: str = "USD",
        end_datetime: str = "",
        use_rth: int = 1,
        timeout_seconds: int = 30,
        source: str = "interactive_brokers",
    ) -> pd.DataFrame:
        """
        Fetch bars for multiple tickers and return one concatenated DataFrame.
        """
        frames: list[pd.DataFrame] = []

        for ticker in tickers:
            df = self.fetch_stock_data(
                ticker=ticker,
                duration=duration,
                bar_size=bar_size,
                exchange=exchange,
                primary_exchange=primary_exchange,
                sec_type=sec_type,
                currency=currency,
                end_datetime=end_datetime,
                use_rth=use_rth,
                timeout_seconds=timeout_seconds,
                source=source,
            )

            if not df.empty:
                frames.append(df)

        if not frames:
            return pd.DataFrame(
                columns=[
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
            )

        return pd.concat(frames, ignore_index=True)