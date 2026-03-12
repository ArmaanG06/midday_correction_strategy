create schema if not exists market_data;

create table if not exists market_data.assets (
    asset_id bigserial primary key,
    symbol text unique not null,
    asset_type text,
    exchange text,
    created_at timestamptz not null default now()
);

create table if not exists market_data.intraday_bars (
    bar_id bigserial primary key,
    symbol text not null references market_data.assets(symbol),
    bar_ts timestamptz not null,
    bar_size text not null,
    open numeric(18,6) not null,
    high numeric(18,6) not null,
    low numeric(18,6) not null,
    close numeric(18,6) not null,
    volume bigint not null,
    vwap numeric(18,6),
    trade_count bigint,
    source text,
    created_at timestamptz not null default now(),
    unique(symbol, bar_ts, bar_size)
);