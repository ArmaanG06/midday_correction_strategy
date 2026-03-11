create index if not exists idx_intraday_bars_symbol_ts
    on market_data.intraday_bars(symbol, bar_ts);

create index if not exists idx_intraday_bars_symbol_bar_size_ts
    on market_data.intraday_bars(bar_ts);