import os
import json
import time
from typing import Dict
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockLatestTradeRequest


import boto3

STREAM_NAME = os.getenv("STREAM_NAME", "")
SYMBOL = os.getenv("SYMBOL", "MSFT")
DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"

kinesis = boto3.client("kinesis")

def fetch_stock_price(symbol: str) -> Dict:
    """
    Fetches the latest stock price for a given symbol using Alpaca API.

    Parameters:
        symbol (str): The stock symbol to fetch the price for.
        api_key (str): Your Alpaca API key.
        api_secret (str): Your Alpaca API secret.

    Returns:
        float: The latest stock price.
    """
    request = StockLatestTradeRequest(symbol_or_symbols=symbol)
    latest_trade = get_alpaca_client(secret_name="AlpacaApiSecret").get_stock_latest_trade(request)

    # Extract the latest ask price from the response
    latest_price = latest_trade[symbol].price
    return {"symbol": symbol, "event_time_millis": int(time.time() * 1000), "price": latest_price}


def get_alpaca_client(secret_name: str) -> StockHistoricalDataClient:
    client = boto3.client('secretsmanager')
    response = client.get_secret_value(SecretId=secret_name)
    secret = json.loads(response['SecretString'])
    return StockHistoricalDataClient(secret['api_key'], secret['api_secret'])


def put_to_kinesis(record: Dict):
    """
    Put a single record to Kinesis.
    """
    if not STREAM_NAME:
        raise RuntimeError("STREAM_NAME environment variable is required")

    kinesis.put_record(
        StreamName=STREAM_NAME,
        Data=json.dumps(record, separators=(",", ":"), ensure_ascii=False).encode("utf-8"),
        PartitionKey=record.get("symbol", "pk")
    )


def handler(event: Dict, context: Dict):
    price_record = fetch_stock_price(SYMBOL)

    if DRY_RUN:
        # Print to logs instead of Kinesis
        return {"status": "ok", "dry_run": True}
    
    put_to_kinesis(price_record)
    return {"status": "ok", "dry_run": False}


# Local convenience: run as a script for quick checks (no AWS calls when DRY_RUN=true)
if __name__ == "__main__":
    os.environ["DRY_RUN"] = "true"
    print(handler({}, None))
