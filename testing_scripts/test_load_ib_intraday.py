import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.mrs.data.main_load import load_ib_intraday_data

def main():
    symbols = ["git LMT"]
    duration = "1 D"
    bar_size = "1 min"
    df = load_ib_intraday_data(symbols=symbols, duration=duration, bar_size=bar_size)
    print(df.head())

if __name__ == "__main__":
    main()