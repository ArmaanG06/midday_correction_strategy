from src.mrs.data.nd_load import IBFetcher
from src.mrs.data.stdize import standardize_bars
from src.mrs.data.postgres_w import upsert_intraday_bars, upsert_assets

import pandas as pd


def load_ib_intraday_data(symbols):

    fetcher = IBFetcher()
    fetcher.connect_app()

    try:
        clean_df = fetcher.fetch_multiple_stock_data(symbols, duration="4 D", bar_size="15 mins")
    finally:
        fetcher.disconnect_app()
    

    assets = pd.DataFrame([
        {"symbol": s, "name": None, "asset_type": "equity", "exchange": "SMART"}
        for s in clean_df["symbol"].unique()
    ])



    upsert_assets(assets)


    numeric_cols = ["open", "high", "low", "close", "volume", "vwap", "trade_count"]
    for col in numeric_cols:
        clean_df[col] = pd.to_numeric(clean_df[col], errors="coerce")    
    upsert_intraday_bars(clean_df)

    return clean_df