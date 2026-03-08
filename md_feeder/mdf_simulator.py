#!/usr/bin/env python3
"""Market Data Feeder — generates synthetic orders with realistic price dynamics.

Price evolution uses regime-based simulation (trending / mean-reverting / volatile)
so that technical indicators (SMA, EMA, MACD, RSI, Bollinger Bands) produce
meaningful, non-random signals for the RL and LLM trading strategies.
"""

import sys
sys.path.insert(0, "/app")

import json, math, time, random, os, threading
from collections import deque

from shared.config import Config
from shared.kafka_utils import create_producer, create_consumer

ORDER_INTERVAL = 60.0 / Config.ORDERS_PER_MIN

# Module-level state (shared with control listener thread)
_securities = {}
_running = True
_suspended = False


# ── Price dynamics engine ─────────────────────────────────────────────────────

class PriceDynamics:
    """Per-symbol state for regime-driven price simulation."""

    REGIMES = ("trending_up", "trending_down", "mean_revert", "volatile", "calm")

    def __init__(self, symbol: str, start_price: float):
        self.symbol = symbol
        self.start_price = start_price
        self.price = start_price
        self.regime = random.choice(self.REGIMES)
        self.regime_ticks = 0
        self.regime_duration = random.randint(20, 60)
        # Rolling history for indicator-aware order generation
        self.history = deque(maxlen=60)
        self.history.append(start_price)
        # Momentum / mean-reversion state
        self.momentum = 0.0
        self.volatility = start_price * 0.005  # ~0.5% base volatility

    def tick(self) -> float:
        """Advance one tick and return the new mid price."""
        tick_size = Config.TICK_SIZE
        self.regime_ticks += 1

        # Switch regime when duration expires
        if self.regime_ticks >= self.regime_duration:
            self._switch_regime()

        # Compute drift + noise based on regime
        if self.regime == "trending_up":
            drift = self.volatility * random.uniform(0.2, 0.8)
            noise = random.gauss(0, self.volatility * 0.5)
            self.momentum = max(self.momentum * 0.95 + drift * 0.05, 0)
        elif self.regime == "trending_down":
            drift = -self.volatility * random.uniform(0.2, 0.8)
            noise = random.gauss(0, self.volatility * 0.5)
            self.momentum = min(self.momentum * 0.95 + drift * 0.05, 0)
        elif self.regime == "mean_revert":
            # Pull toward start price
            pull = (self.start_price - self.price) * 0.03
            drift = pull
            noise = random.gauss(0, self.volatility * 0.3)
            self.momentum *= 0.8
        elif self.regime == "volatile":
            drift = random.gauss(0, self.volatility * 0.5)
            noise = random.gauss(0, self.volatility * 1.5)
            self.momentum *= 0.9
        else:  # calm
            drift = random.gauss(0, self.volatility * 0.1)
            noise = random.gauss(0, self.volatility * 0.2)
            self.momentum *= 0.95

        delta = drift + noise + self.momentum
        # Quantize to tick size
        delta = round(delta / tick_size) * tick_size
        new_price = self.price + delta

        # Floor at 1.00
        new_price = max(1.00, round(new_price, 2))

        # Adaptive volatility: expand in volatile regime, contract in calm
        if self.regime == "volatile":
            self.volatility = min(self.volatility * 1.002, self.price * 0.02)
        elif self.regime == "calm":
            self.volatility = max(self.volatility * 0.998, self.price * 0.002)

        self.price = new_price
        self.history.append(new_price)
        return new_price

    def _switch_regime(self):
        """Transition to a new regime with weighted probabilities."""
        # Bias: if price far from start, favor mean reversion
        deviation = (self.price - self.start_price) / self.start_price
        if abs(deviation) > 0.15:
            weights = [0.05, 0.05, 0.50, 0.20, 0.20]
        elif abs(deviation) > 0.08:
            weights = [0.15, 0.15, 0.30, 0.20, 0.20]
        else:
            weights = [0.25, 0.25, 0.10, 0.15, 0.25]

        self.regime = random.choices(self.REGIMES, weights=weights)[0]
        self.regime_ticks = 0
        self.regime_duration = random.randint(15, 50)

    def sma(self, period: int) -> float:
        h = list(self.history)
        if len(h) < period:
            return sum(h) / len(h)
        return sum(h[-period:]) / period

    def ema(self, period: int) -> float:
        h = list(self.history)
        alpha = 2.0 / (period + 1)
        ema_val = h[0]
        for p in h[1:]:
            ema_val = alpha * p + (1 - alpha) * ema_val
        return ema_val

    def rsi(self, period: int = 14) -> float:
        h = list(self.history)
        if len(h) < 2:
            return 50.0
        gains, losses = [], []
        for i in range(1, len(h)):
            d = h[i] - h[i - 1]
            gains.append(max(d, 0))
            losses.append(max(-d, 0))
        n = min(period, len(gains))
        avg_gain = sum(gains[-n:]) / n if n > 0 else 0
        avg_loss = sum(losses[-n:]) / n if n > 0 else 0.001
        rs = avg_gain / avg_loss if avg_loss > 0 else 100
        return 100 - 100 / (1 + rs)

    def bollinger_position(self, period: int = 20) -> float:
        """Returns 0..1 position within Bollinger Bands (0.5 = at SMA)."""
        h = list(self.history)
        n = min(period, len(h))
        window = h[-n:]
        mean = sum(window) / n
        std = max((sum((x - mean) ** 2 for x in window) / n) ** 0.5, 0.01)
        upper = mean + 2 * std
        lower = mean - 2 * std
        if upper == lower:
            return 0.5
        return max(0.0, min(1.0, (self.price - lower) / (upper - lower)))

    def spread_and_depth(self) -> tuple:
        """Compute adaptive spread and depth quantities based on regime."""
        base_spread = 0.10
        if self.regime == "volatile":
            spread = base_spread * random.uniform(1.5, 2.5)
            depth_qty_range = (30, 100)
        elif self.regime == "calm":
            spread = base_spread * random.uniform(0.6, 1.0)
            depth_qty_range = (100, 300)
        elif self.regime in ("trending_up", "trending_down"):
            spread = base_spread * random.uniform(0.8, 1.5)
            depth_qty_range = (50, 200)
        else:
            spread = base_spread
            depth_qty_range = (80, 250)
        return round(spread, 2), depth_qty_range


_dynamics: dict[str, PriceDynamics] = {}


# ── Securities I/O ────────────────────────────────────────────────────────────

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


def make_snapshot(symbol, best_bid, best_ask, bid_size, ask_size, dyn: PriceDynamics):
    """Build snapshot with embedded technical indicators."""
    return {
        "symbol": symbol,
        "best_bid": round(best_bid, 2),
        "best_ask": round(best_ask, 2),
        "bid_size": bid_size,
        "ask_size": ask_size,
        "timestamp": time.time(),
        "source": "MDF",
        "indicators": {
            "sma_5":  round(dyn.sma(5), 4),
            "sma_20": round(dyn.sma(20), 4),
            "ema_12": round(dyn.ema(12), 4),
            "ema_26": round(dyn.ema(26), 4),
            "macd":   round(dyn.ema(12) - dyn.ema(26), 4),
            "rsi_14": round(dyn.rsi(14), 2),
            "bb_pos": round(dyn.bollinger_position(20), 4),
            "regime": dyn.regime,
        },
    }


def listen_control(ctrl_consumer):
    """Background thread: listen for start/stop/suspend/resume control messages."""
    global _running, _suspended, _securities, _dynamics
    print("[MDF] Control listener started")
    for msg in ctrl_consumer:
        action = (msg.value or {}).get("action")
        if action == "stop":
            _running = False
            _suspended = False
            print("[MDF] STOP signal received – simulation stopped")
        elif action == "start":
            try:
                new_secs = load_securities()
                _securities.clear()
                _securities.update(new_secs)
                # Re-init dynamics for new session
                _dynamics.clear()
                for sym, vals in _securities.items():
                    _dynamics[sym] = PriceDynamics(sym, vals["current"])
                print(f"[MDF] START signal – reloaded securities: {list(_securities.keys())}")
            except Exception as e:
                print(f"[MDF] Error reloading securities on start: {e}")
            _suspended = False
            _running = True
            print("[MDF] Simulation started")
        elif action == "suspend":
            _suspended = True
            print("[MDF] SUSPEND signal received – order generation paused")
        elif action == "resume":
            _suspended = False
            print("[MDF] RESUME signal received – order generation resumed")


if __name__ == "__main__":
    producer = create_producer(component_name="MDF")

    # Load securities and init dynamics
    _securities = load_securities()
    for sym, vals in _securities.items():
        vals["start"] = vals["current"]
        _dynamics[sym] = PriceDynamics(sym, vals["current"])
    save_securities(_securities)
    print(f"[MDF] Loaded securities: {list(_securities.keys())}")

    # Start control consumer in background thread
    try:
        ctrl_consumer = create_consumer(
            topics=[Config.CONTROL_TOPIC],
            group_id="md-feeder-control",
            component_name="MDF-Control",
            auto_offset_reset="latest",
        )
        threading.Thread(target=listen_control, args=(ctrl_consumer,), daemon=True).start()
    except Exception as e:
        print(f"[MDF] Warning: could not start control consumer: {e}")

    try:
        while True:
            if not _running or _suspended:
                time.sleep(0.5)
                continue

            for sym, vals in list(_securities.items()):
                if not _running or _suspended:
                    break

                dyn = _dynamics.get(sym)
                if not dyn:
                    continue

                # Advance price via regime engine
                mid = dyn.tick()
                vals["current"] = mid
                tick = Config.TICK_SIZE
                spread, depth_qty_range = dyn.spread_and_depth()
                half_spread = spread / 2

                # ── Indicator-aware order bias ────────────────────────────
                rsi = dyn.rsi(14)
                bb_pos = dyn.bollinger_position(20)
                macd = dyn.ema(12) - dyn.ema(26)

                # Bias aggressive side based on indicators
                if rsi > 70 and bb_pos > 0.85:
                    aggr_bias = "SELL"       # overbought → sellers step in
                elif rsi < 30 and bb_pos < 0.15:
                    aggr_bias = "BUY"        # oversold → buyers step in
                elif macd > 0 and dyn.regime == "trending_up":
                    aggr_bias = "BUY"
                elif macd < 0 and dyn.regime == "trending_down":
                    aggr_bias = "SELL"
                else:
                    aggr_bias = random.choice(["BUY", "SELL"])

                # ── Place resting depth on both sides ─────────────────────
                for depth_level in range(3):
                    offset = random.randint(1 + depth_level * 3, 3 + depth_level * 5) * tick
                    bid_price = round(mid - half_spread - offset, 2)
                    ask_price = round(mid + half_spread + offset, 2)
                    bid_qty = random.randint(*depth_qty_range)
                    ask_qty = random.randint(*depth_qty_range)

                    # In trending regimes, skew depth: thicker on the passive side
                    if dyn.regime == "trending_up" and depth_level == 0:
                        bid_qty = int(bid_qty * 1.5)
                    elif dyn.regime == "trending_down" and depth_level == 0:
                        ask_qty = int(ask_qty * 1.5)

                    bid_order = make_order(sym, "BUY", bid_price, bid_qty)
                    ask_order = make_order(sym, "SELL", ask_price, ask_qty)
                    producer.send(Config.ORDERS_TOPIC, bid_order)
                    producer.send(Config.ORDERS_TOPIC, ask_order)

                # ── Aggressive orders to generate trades (20-35%) ─────────
                aggr_prob = 0.35 if dyn.regime == "volatile" else 0.20
                if random.random() < aggr_prob:
                    side = aggr_bias
                    if side == "BUY":
                        price = round(mid + half_spread + random.randint(1, 3) * tick, 2)
                    else:
                        price = round(mid - half_spread - random.randint(1, 3) * tick, 2)
                    qty = random.randint(*depth_qty_range)
                    aggr_order = make_order(sym, side, price, qty)
                    producer.send(Config.ORDERS_TOPIC, aggr_order)

                # ── Persist and publish snapshot with indicators ───────────
                save_securities(_securities)
                best_bid = round(mid - half_spread, 2)
                best_ask = round(mid + half_spread, 2)
                bid_size = random.randint(100, 400)
                ask_size = random.randint(100, 400)
                snap = make_snapshot(sym, best_bid, best_ask, bid_size, ask_size, dyn)
                producer.send(Config.SNAPSHOTS_TOPIC, snap)

                time.sleep(ORDER_INTERVAL)

    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
