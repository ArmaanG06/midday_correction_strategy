from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.common import BarData

import threading
import time
import pandas as pd
import os
from typing import List, Union
import json
from datetime import datetime

class IBFetcher(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.data = []
        self.finished = threading.Event()
        self.req_id = 1
        self.database_path= "XXXX"

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

    def fetch_stock_data(self, stock, duration="2 D", bar_size="1 mins", exchange="SMART/AMEX",sec_type="STK", currency="USD", endDateTime=""):
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

        #Make code to save to database & parquets
        #call standardizer before saving data to database and returning to client

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

    
