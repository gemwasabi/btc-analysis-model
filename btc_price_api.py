import requests
from datetime import datetime, timedelta, timezone
import time

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

def main():
    latest_timestamp = None  # Load this from DB or a file if you want persistence

    # Initial load
    if not latest_timestamp:
        start = get_midnight_utc()
        end = datetime.now(timezone.utc)
        candles = fetch_candles(start, end)
        for c in candles:
            # Save candle to DB here
            print(f"Initial load: {datetime.fromtimestamp(c[0], timezone.utc)} Price: {c[4]}")
        if candles:
            latest_timestamp = candles[-1][0]

    # Then poll every 5 minutes for new candles
    while True:
        now = datetime.now(timezone.utc)
        if latest_timestamp and now.timestamp() - latest_timestamp >= 300:
            # fetch only the latest candle
            start = datetime.fromtimestamp(latest_timestamp + 300, timezone.utc)
            end = now
            candles = fetch_candles(start, end)
            for c in candles:
                # Save candle to DB here
                print(f"New candle: {datetime.fromtimestamp(c[0], timezone.utc)} Price: {c[4]}")
                latest_timestamp = c[0]
        else:
            print("Waiting for next candle...")
        time.sleep(60)  # check every minute to catch new candle ASAP

if __name__ == "__main__":
    main()
