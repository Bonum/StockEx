#!/usr/bin/env python3
import time, json, random, requests, os
FRONTEND = os.environ.get("FRONTEND_URL", "http://frontend:5000")
SYMBOLS = ["FOO","AAA","BBB"]
SIDES = ["buy","sell"]

def post_order(order):
    try:
        r = requests.post(FRONTEND + "/submit", json=order, timeout=5)
        print(json.dumps({"component":"oeg","event":"post_order","payload":{"order":order,"status":r.status_code}}))
    except Exception as e:
        print(json.dumps({"component":"oeg","event":"post_failed","payload":{"order":order,"error":str(e)}}))

if __name__ == "__main__":
    try:
        while True:
            order = {
                "order_id": str(int(time.time()*1000)),
                "symbol": random.choice(SYMBOLS),
                "type": random.choice(SIDES),
                "quantity": random.choice([5,10,20]),
                "price": round(100 + random.uniform(-1,1),2),
                "timestamp": time.time(),
                "source": "oeg-sim"
            }
            post_order(order)
            time.sleep(2.0)
    except KeyboardInterrupt:
        pass
