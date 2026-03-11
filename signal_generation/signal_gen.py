from dataclasses import dataclass
#import loader from #Zakirs data puller
import pandas as pd

@dataclass
class SignalParams:
    rebound_threshold: float
    vwap_deviation_threshold: float
    volume_z_threshold: float
    start_time: str
    end_time: str
    max_hold_bars: int

def generate_signals_from_df(bars: pd.DataFrame, params: SignalParams,) -> pd.DataFrame:
    # pure function: assumes bars already filtered
    pass

class BarsLoader:
    def load(self, symbol: str, start: str, end: str, bar_size: str, columns: list[str] | None = None) -> pd.DataFrame:
        #SQL COMMAND TO INTERACT WITH MARKET-DATA SUPABASE DB TO GRAB DATA IN THE FORM OF A PANDAS DF
        pass


def generate_signals(symbol: str, start: str, end: str, bar_size: str, params: SignalParams) -> pd.DataFrame:
    bars = loader.load(symbol, start, end, bar_size,columns=["symbol", "time_stamp_tz", "bar_size", "o", "h", "l", "c", "v", "vwap"])
    #Trial params (should actually me in the main file where func is called but is here for now):
    params = SignalParams(
    rebound_threshold=0.02,
    vwap_deviation_threshold=-0.015,
    volume_z_threshold=2.0,
    start_time="11:00",
    end_time="13:30",
    max_hold_bars=20
)
    return generate_signals_from_df(bars, params)