import sys
sys.path.insert(0, "/app")

import json
from shared.config import Config
from shared.kafka_utils import create_consumer


consumer = create_consumer(
    topics=Config.TRADES_TOPIC,
    group_id="order-group",
    auto_offset_reset="earliest",
    component_name="Consumer"
)

print("Listening for trades...")

for msg in consumer:
    trade = msg.value
    qty = trade.get("quantity") or trade.get("qty") or "-"
    price = trade.get("price", "-")
    print(f"TRADE: {trade.get('symbol', '?')} - {qty} @ {price}")
