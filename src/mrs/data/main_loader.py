"""
IBKR historical bars fetcher + parquet cache for Midday rebound strat

canonical bars schema returned by load_bars / fetch_bars
    ts (datetime64[ns, UTC]), symbol (str),
    open, high, low, close (float 64), volume (int64)

This module:
- fetches bars from IBAPI (TWS gateway must be running)
- catches to parquet under: data/parquet/bars/dataset=XXX/symbol=XXX/bar_size=MH/year=YYYY/month=0X
- can load cahced parquet back into canocial schema

"""

from __future__ import annotations
from datetime import datetime
from existing_data_loader import pull_existing_data
from new_data_loader import IBFetcher

import pandas as pd

def pull_data(ticker, start_date, end_date, bar_interval, base_path, data_set="us_equities", exchange="SMART/AMEX",sec_type="STK", currency="USD"):
    data = pd.DataFrame
    ib = IBFetcher()
    ib.connect_app()
    date_range = pd.date_range(start=start_date, end=end_date, freq=bar_interval,)
    for i in range(len(date_range)):
        df = pull_existing_data(ticker, date_range[i], date_range[i+1], bar_interval, base_path, data_set)
        duration = calculate_duration(start_date, end_date)
        if df.empty:
            df = ib.fetch_stock_data(ticker, duration=duration, bar_size=bar_interval, currency=currency, exchange=exchange, sec_type=sec_type, )
        data = pd.concat([data, df])

    ib.disconnect()

    return data

def calculate_duration(start_date, end_date):
    if isinstance(start_date, str):
        start_date = datetime.fromisoformat(start_date)
    if isinstance(end_date, str):
        end_date = datetime.fromisoformat(end_date)

    delta = end_date - start_date
    days = delta.days

    if days <= 0:
        raise ValueError("end_date must be after start_date")

    if days < 7:
        return f"{days} D"
    elif days < 30:
        return f"{days // 7} W"
    elif days < 365:
        return f"{days // 30} M"
    else:
        return f"{days // 365} Y"
