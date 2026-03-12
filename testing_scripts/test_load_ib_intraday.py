import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.mrs.data.main_load import load_ib_intraday_data

def main():
    symbols = [str(input("Enter your ticker: "))]
    duration =str(input("Enter your duration (IB Compatble): "))
    bar_size = str(input("Enter your bar-size (IB Compatable): "))
    df = load_ib_intraday_data(symbols=symbols, duration=duration, bar_size=bar_size)
    print(df.head())

if __name__ == "__main__":
    main()