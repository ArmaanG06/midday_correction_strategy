import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.mrs.signal_generation.signal_gen import generate_signals
from src.mrs.configs import params_15m

stocks = ["NVDA"]


raw, clean = generate_signals(stocks, "1 W", "15 mins", params=params_15m)
print("------------------------------------------------------------------------------------")
print(raw)
print("------------------------------------------------------------------------------------")
print(clean)
print("------------------------------------------------------------------------------------")
