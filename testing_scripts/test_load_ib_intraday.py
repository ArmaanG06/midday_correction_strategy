import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.mrs.data.main_load import load_ib_intraday_data

def main():
    symbols = ["AAPL", "MSFT"]
    df = load_ib_intraday_data(symbols=symbols)
    print(df.head())

if __name__ == "__main__":
    main()