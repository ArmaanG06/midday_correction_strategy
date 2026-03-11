from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.common import BarData
import duckdb
from pathlib import Path

import threading
import time
import pandas as pd
import os
from typing import List, Union
import json
from datetime import datetime

from standardize import standardize_bars
from parition_data import partition_data

def save_to_datalake(partitioned_data, base_path, dataset,  mode: str="append"):
    conn = duckdb.connect(database=":memory:")

    for (ticker, bar_interval, year, month, day), df in partitioned_data.items():
        partition_path = (Path(base_path)/f"dataset={dataset}"/f"bar_interval={bar_interval}"/f"ticker={ticker}"/f"year={year}"/f"month={month:02d}"/f"day={day:02d}")

        if partition_path.exists():
            raise RuntimeError(f"Parition already exists (append-only violation): {partition_path}")
        partition_path.mkdir(parents=True, exists_ok = False)
        conn.register("df", df)
        conn.execute(f"""
        COPY df
        TO '{partition_path}'
        (FORMAT PARQUET);
        """)
        conn.unregister("df")
    conn.close()

class IBFetcher(EWrapper, EClient):
    def __init__(self, base_path):
        EClient.__init__(self, self)
        self.data = []
        self.finished = threading.Event()
        self.req_id = 1
        self.base_path= base_path

    def connect_path(self, host="127.0.0.1", port=7496, client_id=1):
        print("Connecting to IB...")
        self.connect(host, port, client_id)
        
        thread = threading.Thread(target=self.run, daemon=True)
        thread.start()

        time.sleep(1)
        print("Connected.")

    def disconnect_app(self):
        self.disconnect()
        print("Disconnected from IB.")

    def fetch_stock_data(self, ticker, duration="2 D", bar_size="1 mins", exchange="SMART/AMEX",sec_type="STK", currency="USD", endDateTime="", dataset="us_equities"):
        print("Pulling new data.")
        symbol = symbol.replace('.', '-')
        self.data = []
        self.finished.clear()

        contract = Contract()
        contract.symbol = symbol
        contract.secType = sec_type
        contract.exchange = exchange
        contract.currency = currency

        self.reqHistoricalData(
            reqId = self.req_id,
            contract = contract,
            endDateTime = endDateTime,
            durationStr = duration,
            barSizeSetting = bar_size,
            whatToShow="TRADES",
            useRTH=1,
            formatDate=1,
            keepUpToDate=False,
            chartOptions=[]
        )

        if not self.finished.wait(timeout=30):
            raise TimeoutError(f"Timeout fetching data for {symbol}")
        
        df = pd.DataFrame(self.data, columns=['datetime', 'open', 'high', 'low', 'close', 'volume'])


        partitioned_data = partition_data(df, ticker, bar_size)
        save_to_datalake(partitioned_data, self.base_path, contract, dataset=dataset)
    

        return df
    
    def fetch_multiple_stock_data(self, tickers, duration = "2 D", bar_size="1 mins", endDateTime=""):
        data = pd.DataFrame
        for ticker in tickers:
            stock_data = self.fetch_stock_data(ticker, duration = duration, bar_size=bar_size, endDateTime=endDateTime)
            if stock_data is not None:
                data[ticker] = stock_data
        return data

    def historicalDataEnd(self, reqId: int, start: str, end: str):
        print(f"Data collection finished for request {reqId}")
        self.finished.set()


    
