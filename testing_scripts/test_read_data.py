from src.mrs.data.postgres_r import load_intraday_bars


def main():
    df = load_intraday_bars(
        symbol="AAPL",
        start_ts="2026-03-10 00:00:00+00:00",
        end_ts="2026-03-11 00:00:00+00:00",
        bar_size="5m",
    )

    print(df)


if __name__ == "__main__":
    main()