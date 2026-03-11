from src.mrs.data.nd_load import IBFetcher
from src.mrs.data.stdize import standardize_bars
from src.mrs.data.postgres_w import upsert_intraday_bars, upsert_assets

import pandas as pd


def load_ib_intraday_data(symbols):

    fetcher = IBFetcher()
    fetcher.connect_app()

    try:
        raw_df = fetcher.fetch_multiple_stock_data(symbols)
    finally:
        fetcher.disconnect_app()

    clean_df = standardize_bars(raw_df)

    assets = pd.DataFrame([
        {"symbol": s, "name": None, "asset_type": "equity", "exchange": "SMART"}
        for s in clean_df["symbol"].unique()
    ])

    upsert_assets(assets)
    upsert_intraday_bars(clean_df)

    return clean_df