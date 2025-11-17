import os
import alpaca_trade_api as tradeapi
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load credentials
load_dotenv()

try:
    alpaca_api = tradeapi.REST(os.environ['ALPACA_API_KEY'], 
                                os.environ['ALPACA_SECRET'], 
                                'https://paper-api.alpaca.markets/v2')
except KeyError as e:
    print(f'Error: {e}')
    exit(1)

# Fetch the data
symbols = ['AAPL', 'GOOGL', 'SPY']
start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

print(f"Fetching data from {start_date} to {end_date}...")
bars = alpaca_api.get_bars(symbols, '1Min', start=start_date, end=end_date).df

# Inspect the structure
print("\n=== DataFrame Info ===")
print(f"Shape: {bars.shape}")
print(f"\nColumns: {bars.columns.tolist()}")
print(f"\nIndex: {bars.index}")
print(f"Index name: {bars.index.name}")
print(f"Index type: {type(bars.index)}")

print("\n=== First few rows ===")
print(bars.head())

print("\n=== Inspecting first row ===")
first_row = bars.iloc[0]
print(f"First row index: {bars.index[0]} (type: {type(bars.index[0])})")
print(f"First row data:")
print(first_row)