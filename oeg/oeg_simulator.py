#!/usr/bin/env python3
import time, json, random, requests, os

FRONTEND = os.environ.get("FRONTEND_URL", "http://frontend:5000")
SECURITIES_FILE = os.environ.get("SECURITIES_FILE", "/app/shared/data/securities.txt")
SIDES = ["buy", "sell"]


def load_securities():
    securities = {}
    try:
        with open(SECURITIES_FILE) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    parts = line.split()
                    if len(parts) >= 3:
                        securities[parts[0]] = float(parts[2])
    except Exception:
        pass
    return securities or {"ALPHA": 5.65, "PEIR": 8.35, "EXAE": 6.90}


def post_order(order):
    try:
        r = requests.post(FRONTEND + "/submit", json=order, timeout=5)
        print(json.dumps({"component": "oeg", "event": "post_order", "payload": {"order": order, "status": r.status_code}}))
    except Exception as e:
        print(json.dumps({"component": "oeg", "event": "post_failed", "payload": {"order": order, "error": str(e)}}))


if __name__ == "__main__":
    securities = load_securities()
    symbols = list(securities.keys())
    print(json.dumps({"component": "oeg", "event": "loaded_securities", "payload": {"symbols": symbols}}))
    try:
        while True:
            symbol = random.choice(symbols)
            base_price = securities[symbol]
            order = {
                "order_id": str(int(time.time() * 1000)),
                "symbol": symbol,
                "type": random.choice(SIDES),
                "quantity": random.choice([5, 10, 20]),
                "price": round(base_price + random.uniform(-0.5, 0.5), 2),
                "timestamp": time.time(),
                "source": "oeg-sim"
            }
            post_order(order)
            time.sleep(2.0)
    except KeyboardInterrupt:
        pass
