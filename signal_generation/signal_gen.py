from dataclasses import dataclass
#from Zakirs data puller import loader once he is done
import pandas as pd

COLUMN_NAMES = ["ticker", "timestamp", "entry", "position", "price", "amount"]

params_1m = {
    "bar_size": 1,
    "price_inc" : 0.05,
    "vol_inc" : 0.05,
    "entry_time_1" : 1, #hours into session
    "exit_time_1" : 3
}

params_5m = {
    "bar_size": 5,
    "price_inc" : 0.05,
    "vol_inc" : 0.05,
    "entry_time_1" : 1.5, #hours into session
    "exit_time_1" : 3
}

params_15m = {
    "bar_size": 15,
    "price_inc" : 0.05,
    "vol_inc" : 0.05,
    "entry_time_1": 0.5, #hours into session
    "exit_time_1": 4,
    "entry_time_2": 3,
    "exit_time_2": 6
}



def generate_signals_from_df(bars: pd.DataFrame, params, param_version:"1"):
    
    signals_df = pd.DataFrame(columns=COLUMN_NAMES)

    bar_size = params["bar_size"]
    bar_delta_entry = params[f"entry_time_{param_version}"] * (60/bar_size)
    bar_delta_exit = params[f"exit_time_{param_version}"] * (60/bar_size)
    
    price_change = (bars[bar_delta_entry]["open"] - bars[0]["open"])/bars[0]["open"]
    vol_change = (bars[bar_delta_entry]["volume"] - bars[0]["volume"])/bars[0]["volume"]

    if price_change > params["price_inc"] and vol_change > params["price_inc"]:
        signals_df = []


    pass


def clean_signals(signals_raw):
    pass


def generate_signals(symbol: str, start: str, end: str, bar_size: str, params) -> pd.DataFrame:
    bars = loader.load(symbol, start, end, bar_size)

    #Trial params (should actually me in the main file where func is called but is here for now):
    
    signals_raw = generate_signals_from_df(bars, params, "1") #will return a df that contains more information
    signals_pure = clean_signals(signals_raw) #will return a df that only contains the entry (1,0,-1) and position (1,0,-1)
    return signals_raw, signals_pure


