#!/usr/bin/env python3
import sys
sys.path.insert(0, "/app")

import json, time, random, os

from shared.config import Config
from shared.kafka_utils import create_producer

ORDER_INTERVAL = 60.0 / Config.ORDERS_PER_MIN

def load_securities():
    """Load securities from file: SYMBOL start_price current_price"""
    securities = {}
    if not os.path.exists(Config.SECURITIES_FILE):
        raise FileNotFoundError(f"{Config.SECURITIES_FILE} not found")

    with open(Config.SECURITIES_FILE) as f:
        for line in f:
            if not line.strip() or line.startswith("#"):
                continue
            parts = line.split()
            if len(parts) >= 3:
                symbol, start, current = parts[0], float(parts[1]), float(parts[2])
                securities[symbol] = {"start": start, "current": current}
    return securities

def save_securities(securities):
    """Persist securities with header line"""
    with open(Config.SECURITIES_FILE, "w") as f:
        f.write("#SYMBOL\t<start_price>\t<current_price>\n")
        for sym, vals in securities.items():
            f.write(f"{sym}\t{vals['start']:.2f}\t{vals['current']:.2f}\n")

_order_counter = 0

def make_order(symbol, side, price, qty):
    global _order_counter
    _order_counter += 1
    return {
        "symbol": symbol,
        "side": side,
        "price": round(price, 2),
        "quantity": qty,
        "cl_ord_id": f"MDF-{int(time.time()*1000)}-{_order_counter}",
        "timestamp": time.time(),
        "source": "MDF"
    }

def make_snapshot(symbol, best_bid, best_ask, bid_size, ask_size):
    return {
        "symbol": symbol,
        "best_bid": round(best_bid, 2),
        "best_ask": round(best_ask, 2),
        "bid_size": bid_size,
        "ask_size": ask_size,
        "timestamp": time.time(),
        "source": "MDF"
    }

if __name__ == "__main__":
    producer = create_producer(component_name="MDF")

    # Load securities and reset start=current
    securities = load_securities()
    for sym in securities:
        securities[sym]["start"] = securities[sym]["current"]
    save_securities(securities)
    print(f"[MDF] Loaded securities: {list(securities.keys())}")

    try:
        while True:
            for sym, vals in securities.items():
                mid = vals["current"]
                # Define spread: bids below (mid - spread), asks above (mid + spread)
                half_spread = 0.10  # 10 cents spread

                # --- generate order ---
                # 90% passive orders (build book), 10% aggressive (create trades)
                side = random.choice(["BUY", "SELL"])
                rand = random.random()

                if rand < 0.90:
                    # Passive: place orders away from mid to rest on book
                    if side == "BUY":
                        # Bids: from (mid - spread) down to (mid - spread - 0.50)
                        base = mid - half_spread
                        offset = random.randint(1, 50) * Config.TICK_SIZE
                        price = round(base - offset, 2)
                    else:
                        # Asks: from (mid + spread) up to (mid + spread + 0.50)
                        base = mid + half_spread
                        offset = random.randint(1, 50) * Config.TICK_SIZE
                        price = round(base + offset, 2)
                else:
                    # Aggressive: cross the spread to create trades (rare)
                    if side == "BUY":
                        # Buy into asks
                        price = round(mid + half_spread + random.randint(1, 5) * Config.TICK_SIZE, 2)
                    else:
                        # Sell into bids
                        price = round(mid - half_spread - random.randint(1, 5) * Config.TICK_SIZE, 2)

                qty = random.choice([50, 100, 150, 200, 250])

                order = make_order(sym, side, price, qty)
                producer.send(Config.ORDERS_TOPIC, order)
                print(f"[MDF] Order: {order}")

                # --- simulate small price drift (10% chance, max 2 ticks) ---
                if random.random() < 0.10:
                    drift = random.choice([-2, -1, 1, 2]) * Config.TICK_SIZE
                    new_price = vals["current"] + drift
                    # Keep price within reasonable bounds (min 1.00)
                    if new_price >= 1.00:
                        vals["current"] = round(new_price, 2)
                        save_securities(securities)

                # --- snapshot around mid price ---
                best_bid = mid - half_spread
                best_ask = mid + half_spread
                bid_size = random.choice([50, 100, 200])
                ask_size = random.choice([50, 100, 200])
                snap = make_snapshot(sym, best_bid, best_ask, bid_size, ask_size)
                producer.send(Config.SNAPSHOTS_TOPIC, snap)
                print(f"[MDF] Snapshot: {snap}")

                time.sleep(ORDER_INTERVAL)

    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
