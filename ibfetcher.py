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
        EClient.__init__(self,self)
        self.data = []
        self.finished = threading.Event()
        self.req_id = 1
        self.raw_path = "./data/raw"
        self.processed_path = "./data/processed"

    def _raw_filepath(self, duration, bar_size, ticker: str) -> str:
        now = datetime.now().strftime("%Y%m%d")

        return os.path.join(self.raw_path, f"{ticker}_{duration}_{bar_size}_{now}.csv")

    def connect_app(self, host='127.0.0.1', port=7496, client_id=1):
        print("Connecting to IB...")
        self.connect(host, port, client_id)

        thread = threading.Thread(target=self.run, daemon=True)
        thread.start()

        time.sleep(1)
        print("Connected.")

    def disconnect_app(self):
        self.disconnect()
        print("Disconnected from IB.")

    def fetch_stock_data(self, symbol, duration="2 D", bar_size="5 mins", exchange="SMART/AMEX", sec_type="STK", currency="USD", endDateTime=""):
        symbol = symbol.replace('.', '-')
        filepath = self._raw_filepath(duration=duration, bar_size=bar_size, ticker=symbol)
        if os.path.exists(filepath):
            print(f"Loading {symbol} from cache.")
            return pd.read_csv(filepath)

        self.data = [] 
        self.finished.clear()

        contract = Contract()
        contract.symbol = symbol
        contract.secType = sec_type
        contract.exchange = exchange
        contract.currency = currency

        self.reqHistoricalData(
            reqId=self.req_id,
            contract=contract,
            endDateTime=endDateTime,
            durationStr=duration,
            barSizeSetting=bar_size,
            whatToShow="TRADES",
            useRTH=1,
            formatDate=1,
            keepUpToDate=False,
            chartOptions=[]
        )

        # 3. Wait for the data to finish
        if not self.finished.wait(timeout=30):
            raise TimeoutError(f"Timeout fetching data for {symbol}")

        # 4. Convert to DataFrame
        df = pd.DataFrame(self.data, columns=['datetime', 'open', 'high', 'low', 'close', 'volume'])

        # 5. Save to CSV for caching
        os.makedirs(self.raw_path, exist_ok=True)
        df.to_csv(filepath, index=False)
        print(f"Saved {symbol} to {filepath}")

        return df

    def fetch_multiple_stock_data(self, tickers, duration='2 D', bar_size='5 mins', endDateTime=""):
        data = {}
        for ticker in tickers:
            stock_data = self.fetch_stock_data(ticker, duration=duration, bar_size=bar_size, endDateTime=endDateTime)
            if stock_data is not None:
                data[ticker] = stock_data

        return data
        
    def fetch_sp_500_data(self, duration='2 D', bar_size='5 mins') -> dict:
        sp500 = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")[0]
        sp500 = sp500['Symbol'].to_list()
        sp500_data = self.fetch_multiple_stock_data(sp500, duration=duration, bar_size=bar_size)
        return sp500_data
    
    def fetch_sp500_data_df(self, duration='2 D', bar_size='5 mins') -> pd.DataFrame:
        sp500 = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")[0]['Symbol'].tolist()
        sp500 = [t.replace('.', '-') for t in sp500]
        sp500_dict = self.fetch_multiple_stock_data(sp500, duration=duration, bar_size=bar_size)
        
        df_list = []
        for ticker, df in sp500_dict.items():
            df['ticker'] = ticker
            df_list.append(df)
        
        return pd.concat(df_list, ignore_index=True)

        
    def historicalData(self, reqId: int, bar: BarData):
        self.data.append([
            bar.date, bar.open, bar.high, bar.low, bar.close, bar.volume
        ])

    # Callback for when historical data is finished
    def historicalDataEnd(self, reqId: int, start: str, end: str):
        print(f"Data collection finished for request {reqId}")
        self.finished.set()

    # Optional: handle errors
    def error(self, reqId, errorCode, errorString):
        print(f"Error {errorCode}: {errorString}")