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

import os
import time
import threading

from datetime import datetime, timezone

import pandas as pd

#def pull data(ticker, start_date, end_date, bar_interval):

    #mapping params to IB compatable params to be able to access data from api and database as it will be saved with IB compatable variables

    #Logic to decide if data already exists

    #logic to split data into saved paritions

    #if else to pull new or old data

    #combine new pulled data and old data or combine differetn segments of old data



