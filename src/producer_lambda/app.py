import os
import json
import time
import boto3
import random

kinesis = boto3.client("kinesis")
STREAM_NAME = os.environ["STREAM_NAME"]

def handler(event, context):
    # Example payload; replace with your real data generator
    payload = {
        "ts": int(time.time() * 1000),
        "source": "producer-lambda",
        "sequence": random.randint(1, 1_000_000),
        "data": {"example": "value"}
    }

    kinesis.put_record(
        StreamName=STREAM_NAME,
        Data=json.dumps(payload).encode("utf-8"),
        PartitionKey=str(payload["sequence"])
    )
    return {"status": "ok", "stream": STREAM_NAME}
