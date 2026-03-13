"""
Pure Customer/recruiter layer
metrics does not simulate trades
metrics does not infer missing fills
metrics does not touch the database
metrics takes standardized backtest outputs
metrics returns calculated stats and reports

Inputs (where ever you need): engine.py comments

outputs:
metrics_results = {
    "performance_summary": performance_summary_dict,
    "trade_summary": trade_summary_dict,
    "risk_summary": risk_summary_dict,
    "execution_summary": execution_summary_dict,
    "symbol_summary": symbol_summary_df,
    "drawdown_table": drawdown_table_df,
}

performance_summary = {
    "initial_capital": 100000.0,
    "final_equity": 108450.0,
    "net_pnl": 8450.0,
    "total_return_pct": 8.45,
    "annualized_return_pct": 16.2,
    "annualized_volatility_pct": 11.8,
    "sharpe_ratio": 1.37,
    "sortino_ratio": 1.92,
    "max_drawdown_pct": -4.6,
    "calmar_ratio": 3.52,
    "profit_factor": 1.41
}

trade_summary = {
    "num_trades": 84,
    "num_winning_trades": 46,
    "num_losing_trades": 38,
    "win_rate_pct": 54.76,
    "avg_trade_pnl": 100.6,
    "median_trade_pnl": 48.2,
    "avg_win": 312.4,
    "avg_loss": -155.7,
    "largest_win": 980.0,
    "largest_loss": -640.0,
    "expectancy": 100.6,
    "avg_return_per_trade_pct": 0.22,
    "avg_holding_period_minutes": 47.5
}

risk_summary = {
    "max_drawdown_pct": -4.6,
    "max_drawdown_duration_bars": 38,
    "downside_volatility_pct": 8.3,
    "var_95_pct": -1.2,
    "cvar_95_pct": -1.8,
    "avg_gross_exposure_pct": 43.5,
    "max_gross_exposure_pct": 100.0,
    "avg_net_exposure_pct": 41.2,
    "time_in_market_pct": 52.4
}

execution_summary = {
    "total_commission": 420.0,
    "total_slippage": 315.0,
    "total_costs": 735.0,
    "avg_cost_per_trade": 8.75,
    "turnover": 2.14
}


symbol_summary = {
    symbol
    num_trades
    win_rate_pct
    net_pnl
    avg_trade_pnl
    profit_factor
    avg_holding_period_minutes
}



example for how to do this is with one main function in a possible main.py file in the metrics folder that has:
def compute_all_metrics(backtest_results: dict) -> dict:
    ...
    #call pref_summ()
    #call risk_summ()
    #calls all the smaller functions from other files
    ...

    
Something optional can be to make a function that takes in my data from backtesting engine and validates it before running thru the metrics logic your making
for example runing the data dict thru a def validate_backtest_results(backtest_results: dict) -> boolean: just to check whether or not the data can find metrics on it
to avoid extensive error checking later. from there once oyur metrics are all outputed into a dict we (or you depending on the length of the backtesting work) can
begin the dev of a user dashboard for simplified statistics view and strategy explaination

"""