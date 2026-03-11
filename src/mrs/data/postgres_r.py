import pandas as pd
from sqlalchemy import text
from src.mrs.data.database import get_engine


def load_intraday_bars(symbol: str, start_ts: str, end_ts: str, bar_size: str) -> pd.DataFrame:
    engine = get_engine()

    query = text("""
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
        from market_data.intraday_bars
        where symbol = :symbol
          and bar_ts >= :start_ts
          and bar_ts < :end_ts
          and bar_size = :bar_size
        order by bar_ts
    """)

    with engine.connect() as conn:
        df = pd.read_sql(
            query,
            conn,
            params={
                "symbol": symbol,
                "start_ts": start_ts,
                "end_ts": end_ts,
                "bar_size": bar_size,
            },
        )

    return df