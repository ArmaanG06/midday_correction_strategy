import pandas as pd
from sqlalchemy import text
from src.mrs.data.database import get_engine


def upsert_assets(df: pd.DataFrame):
    if df.empty:
        print("No assets to write.")
        return

    engine = get_engine()

    with engine.begin() as conn:
        df.to_sql(
            name="assets_staging",
            con=conn,
            schema="market_data",
            if_exists="replace",
            index=False,
            method="multi",
            chunksize=1000,
        )

        conn.execute(text("""
            insert into market_data.assets (symbol, name, asset_type, exchange)
            select symbol, name, asset_type, exchange
            from market_data.assets_staging
            on conflict (symbol)
            do update set
                name = excluded.name,
                asset_type = excluded.asset_type,
                exchange = excluded.exchange;
        """))

        conn.execute(text("drop table if exists market_data.assets_staging;"))

    print(f"Wrote {len(df)} assets.")


def upsert_intraday_bars(df: pd.DataFrame):
    if df.empty:
        print("No intraday bars to write.")
        return

    df = df.copy()

    if "bar_ts" not in df.columns:
        raise ValueError("Missing required column: bar_ts")

    df["bar_ts"] = pd.to_datetime(df["bar_ts"], utc=True, errors="raise")

    engine = get_engine()

    with engine.begin() as conn:
        df.to_sql(
            name="intraday_bars_staging",
            con=conn,
            schema="market_data",
            if_exists="replace",
            index=False,
            method="multi",
            chunksize=1000,
        )

        conn.execute(text("""
            insert into market_data.intraday_bars (
                symbol, bar_ts, bar_size, open, high, low, close,
                volume, vwap, trade_count, source
            )
            select
                symbol,
                bar_ts,
                bar_size,
                open,
                high,
                low,
                close,
                volume,
                vwap,
                trade_count,
                source
            from market_data.intraday_bars_staging
            on conflict (symbol, bar_ts, bar_size)
            do update set
                open = excluded.open,
                high = excluded.high,
                low = excluded.low,
                close = excluded.close,
                volume = excluded.volume,
                vwap = excluded.vwap,
                trade_count = excluded.trade_count,
                source = excluded.source;
        """))

        conn.execute(text("drop table if exists market_data.intraday_bars_staging;"))

    print(f"Wrote {len(df)} intraday bars.")