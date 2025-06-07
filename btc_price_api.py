import requests
from datetime import datetime, timedelta, timezone
import time
import csv
import os

def fetch_candles(start, end):
    url = "https://api.exchange.coinbase.com/products/BTC-USD/candles"
    params = {
        "start": start.isoformat(),
        "end": end.isoformat(),
        "granularity": 300
    }
    resp = requests.get(url, params=params)
    resp.raise_for_status()
    data = resp.json()
    data.sort(key=lambda x: x[0])
    return data

def get_midnight_utc():
    now = datetime.now(timezone.utc)
    return now.replace(hour=0, minute=0, second=0, microsecond=0)

def write_to_csv(candle_data):
    file_exists = os.path.isfile("btc_prices.csv")
    with open("btc_prices.csv", mode="a", newline='', encoding="utf-8") as file:
        writer = csv.writer(file)
        if not file_exists:
            writer.writerow(["timestamp", "open", "high", "low", "close", "volume"])
        for c in candle_data:
            timestamp = datetime.fromtimestamp(c[0], timezone.utc).isoformat()
            writer.writerow([timestamp, c[1], c[2], c[3], c[4], c[5]])

def main():
    latest_timestamp = None

    if not latest_timestamp:
        start = get_midnight_utc()
        end = datetime.now(timezone.utc)
        candles = fetch_candles(start, end)
        write_to_csv(candles)
        print(f"Initial load: Saved {len(candles)} candles to btc_prices.csv")
        if candles:
            latest_timestamp = candles[-1][0]

    while True:
        now = datetime.now(timezone.utc)
        if latest_timestamp and now.timestamp() - latest_timestamp >= 300:
            start = datetime.fromtimestamp(latest_timestamp + 300, timezone.utc)
            end = now
            candles = fetch_candles(start, end)
            if candles:
                write_to_csv(candles)
                print(f"New candles: Saved {len(candles)} to btc_prices.csv")
                latest_timestamp = candles[-1][0]
        else:
            print("Waiting for next candle...")
        time.sleep(60)

if __name__ == "__main__":
    main()
