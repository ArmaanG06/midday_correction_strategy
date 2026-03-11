import pandas as pd
from src.mrs.data.postgres_w import upsert_assets, upsert_intraday_bars


def main():
    assets_df = pd.DataFrame([
        {
            "symbol": "AAPL",
            "name": "Apple Inc.",
            "asset_type": "equity",
            "exchange": "NASDAQ",
        }
    ])

    bars_df = pd.DataFrame([
        {
            "symbol": "AAPL",
            "bar_ts": "2026-03-10 14:35:00+00:00",
            "bar_size": "5m",
            "open": 182.10,
            "high": 180.25,
            "low": 179.90,
            "close": 180.00,
            "volume": 1250000,
            "vwap": 180.05,
            "trade_count": 4000,
            "source": "test_script",
        },
        {
            "symbol": "AAPL",
            "bar_ts": "2026-03-10 14:40:00+00:00",
            "bar_size": "5m",
            "open": 180.00,
            "high": 180.15,
            "low": 179.80,
            "close": 179.95,
            "volume": 980000,
            "vwap": 179.98,
            "trade_count": 3200,
            "source": "test_script",
        },
    ])

    bars_df["bar_ts"] = pd.to_datetime(bars_df["bar_ts"], utc=True)

    upsert_assets(assets_df)
    upsert_intraday_bars(bars_df)


if __name__ == "__main__":
    main()