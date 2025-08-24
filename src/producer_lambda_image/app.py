import os
import json
import random
import time
from typing import List, Dict

import boto3

STREAM_NAME = os.getenv("STREAM_NAME", "")
TICKER = os.getenv("TICKER", "MSFT")
PERIOD = os.getenv("PERIOD", "1d")       # e.g., '1d', '5d', '1mo'
INTERVAL = os.getenv("INTERVAL", "1m")   # e.g., '1m', '5m', '15m', '1h', '1d'
DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"

kinesis = boto3.client("kinesis")


def fetch_candles(symbol: str, period: str, interval: str) -> List[Dict]:
    """
    Generate dummy OHLCV data for testing purposes.
    """
    records: List[Dict] = []
    current_time = int(time.time() * 1000)

    for i in range(5):  # Generate 5 dummy records
        ts = current_time - i * 60000  # subtract i minutes
        open_price = round(random.uniform(100, 200), 2)
        high_price = round(open_price + random.uniform(0, 10), 2)
        low_price = round(open_price - random.uniform(0, 10), 2)
        close_price = round(random.uniform(low_price, high_price), 2)
        volume = random.randint(1000, 10000)

        payload = {
            "symbol": symbol,
            "ts": ts,
            "open": open_price,
            "high": high_price,
            "low": low_price,
            "close": close_price,
            "volume": volume,
            "interval": interval,
            "period": period,
            "source": "dummy-generator",
        }
        records.append(payload)

    return records


def put_to_kinesis(records: List[Dict]):
    """
    Batch put to Kinesis using put_records (max 500 per call).
    """
    if not STREAM_NAME:
        raise RuntimeError("STREAM_NAME environment variable is required")

    # Kinesis batch limit is 500
    batch_size = 500
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        entries = [{
            "Data": json.dumps(rec, separators=(",", ":"), ensure_ascii=False).encode("utf-8"),
            "PartitionKey": rec.get("symbol", "pk")
        } for rec in batch]
        kinesis.put_records(StreamName=STREAM_NAME, Records=entries)


def handler(event, context):
    records = fetch_candles(TICKER, PERIOD, INTERVAL)

    if DRY_RUN:
        # Print to logs instead of Kinesis
        return {"status": "ok", "dry_run": True, "count": len(records)}
    
    print(json.dumps({"count": len(records), "sample": records[:3]}, indent=2))
    put_to_kinesis(records)
    return {"status": "ok", "dry_run": False, "count": len(records)}


# Local convenience: run as a script for quick checks (no AWS calls when DRY_RUN=true)
if __name__ == "__main__":
    os.environ["DRY_RUN"] = "true"
    print(handler({}, None))

