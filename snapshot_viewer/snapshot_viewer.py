#!/usr/bin/env python3
import sys
sys.path.insert(0, "/app")

import json
import os
import time
from datetime import datetime

from shared.config import Config
from shared.kafka_utils import create_consumer

# keep only last BBO per symbol
last_bbo = {}

def pretty_print(snapshot):
    bids = snapshot.get("bids", [])
    asks = snapshot.get("asks", [])

    valid_bids = [b for b in bids if b["qty"] > 0]
    valid_asks = [a for a in asks if a["qty"] > 0]

    best_bid = max(valid_bids, key=lambda b: b["price"], default=None)
    best_ask = min(valid_asks, key=lambda a: a["price"], default=None)

    symbol = snapshot.get("symbol", "UNKNOWN")
    now = datetime.utcnow().strftime("%H:%M:%S")

    last_bbo[symbol] = {
        "best_bid": best_bid,
        "best_ask": best_ask,
        "time": now,
    }

    print(f"\n Market Data Snapshot: {symbol} {now}")

    print("\n  BIDS:")
    if best_bid:
        print(f"   {best_bid['qty']:>6} @ {best_bid['price']:.2f}")
    else:
        print("   None")

    print("\n  ASKS:")
    if best_ask:
        print(f"   {best_ask['qty']:>6} @ {best_ask['price']:.2f}")
    else:
        print("   None")

    print("\nBest Bid :", best_bid["price"] if best_bid else "None")
    print("Best Ask:", best_ask["price"] if best_ask else "None")


if __name__ == "__main__":
    consumer = create_consumer(
        topics=Config.SNAPSHOTS_TOPIC,
        group_id="snapshot-viewer",
        component_name="SnapshotViewer"
    )
    print(f"Subscribed to {Config.SNAPSHOTS_TOPIC}, showing only latest BBO per symbol\n")

    for msg in consumer:
        snapshot = msg.value
        if snapshot.get("type") == "snapshot":
            pretty_print(snapshot)
