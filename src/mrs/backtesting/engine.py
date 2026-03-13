"""
Needs to out put:

equity curve -> DF with timestamp, cash, market_value, total_equity
trades log -> df with trade_id, symbol, side, entry_time, exit_time, quantity, entry_price, exit_price, pnl, return_pct
fills log -> df with timestamp, symbol, side, quantity, fill_price, commission, slippage
position history -> df with timestamp, symbol, quantity, avg_price, market_price, unrealized_pnl
cash history 
Drawdown series, or at least enough data to derive it which may be the data above


"""