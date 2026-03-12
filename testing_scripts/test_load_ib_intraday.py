import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.mrs.data.main_load import load_ib_intraday_data

def main():
    symbols = [str(input("Enter Ticker Symbol (ex. AAPL, LMT, MSFT)): "))]
    duration = str(input("Enter Duration (IB Compatble): "))
    bar_size = str(input("Enter Intraday Bar-Size (IB Compatable): "))
    df = load_ib_intraday_data(symbols=symbols, duration=duration, bar_size=bar_size)
    print(df.head())

if __name__ == "__main__":
    main()