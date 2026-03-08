"""RL-based trading strategy using Adilbai/stock-trading-rl-agent (PPO).

Provides decide_order_rl() with the same return type as _decide_order_llm()
so it can be used as a drop-in alternative in ch_ai_trader.py.
"""

import os
import pickle
import random
import threading
import time
from collections import deque
from typing import Optional

import numpy as np

# ── Config ────────────────────────────────────────────────────────────────────
RL_MODEL_REPO = os.getenv("CH_RL_MODEL_REPO", "Adilbai/stock-trading-rl-agent")
RL_MODEL_CACHE = os.getenv("CH_RL_MODEL_CACHE", "/app/data/rl_model")
RL_BAR_INTERVAL = int(os.getenv("CH_RL_BAR_INTERVAL", "60"))  # seconds per bar
RL_MIN_BARS = int(os.getenv("CH_RL_MIN_BARS", "30"))          # min bars before RL kicks in
RL_LOOKBACK = 60

# ── Shared state ──────────────────────────────────────────────────────────────
_model = None
_scaler = None
_model_lock = threading.Lock()
_load_attempted = False

# Per-symbol rolling price bars: {symbol: deque of {open, high, low, close, volume}}
_price_bars: dict[str, deque] = {}
_bars_lock = threading.Lock()
# Accumulator for current bar being built
_current_bar: dict[str, dict] = {}


# ── Model loading ─────────────────────────────────────────────────────────────

def _load_model():
    """Download and load the PPO model + scaler from HuggingFace Hub."""
    global _model, _scaler, _load_attempted
    with _model_lock:
        if _load_attempted:
            return _model is not None
        _load_attempted = True

    try:
        from huggingface_hub import hf_hub_download
        from stable_baselines3 import PPO

        os.makedirs(RL_MODEL_CACHE, exist_ok=True)
        print(f"[CH-RL] Downloading model from {RL_MODEL_REPO}...")

        model_path = hf_hub_download(
            repo_id=RL_MODEL_REPO, filename="final_model.zip",
            cache_dir=RL_MODEL_CACHE,
        )
        scaler_path = hf_hub_download(
            repo_id=RL_MODEL_REPO, filename="scaler.pkl",
            cache_dir=RL_MODEL_CACHE,
        )

        with _model_lock:
            _model = PPO.load(model_path)
            with open(scaler_path, "rb") as f:
                _scaler = pickle.load(f)

        print("[CH-RL] Model loaded successfully")
        return True
    except Exception as e:
        print(f"[CH-RL] Failed to load model: {e}")
        return False


def is_available() -> bool:
    """Check if RL model is loaded and ready."""
    return _model is not None


# ── Price history ─────────────────────────────────────────────────────────────

def feed_trade(symbol: str, price: float, quantity: int):
    """Feed a trade into the price bar accumulator. Called from trade consumer."""
    with _bars_lock:
        if symbol not in _current_bar:
            _current_bar[symbol] = {
                "open": price, "high": price, "low": price,
                "close": price, "volume": quantity,
                "start_time": time.time(),
            }
        else:
            bar = _current_bar[symbol]
            bar["high"] = max(bar["high"], price)
            bar["low"] = min(bar["low"], price)
            bar["close"] = price
            bar["volume"] += quantity

        # Finalize bar if interval elapsed
        bar = _current_bar[symbol]
        if time.time() - bar["start_time"] >= RL_BAR_INTERVAL:
            _finalize_bar(symbol)


def _load_real_ohlcv(symbol: str) -> list[dict]:
    """Try to load real OHLCV bars from shared/data/ohlcv/{symbol}.json."""
    ohlcv_dir = os.getenv("OHLCV_DIR", "/app/shared/data/ohlcv")
    path = os.path.join(ohlcv_dir, f"{symbol}.json")
    if not os.path.exists(path):
        # Also check relative to project root
        alt = os.path.join(os.path.dirname(__file__), "..", "shared", "ohlcv", f"{symbol}.json")
        if os.path.exists(alt):
            path = alt
        else:
            return []
    try:
        import json
        with open(path, "r") as f:
            bars = json.load(f)
        return [
            {"open": b["open"], "high": b["high"], "low": b["low"],
             "close": b["close"], "volume": b["volume"]}
            for b in bars
        ]
    except Exception as e:
        print(f"[CH-RL] Failed to load OHLCV for {symbol}: {e}")
        return []


def seed_price(symbol: str, ref_price: float):
    """Seed initial bars from real OHLCV data if available, else from reference price."""
    with _bars_lock:
        if symbol in _price_bars and len(_price_bars[symbol]) > 0:
            return
        if symbol not in _price_bars:
            _price_bars[symbol] = deque(maxlen=120)

        # Try real OHLCV data first
        real_bars = _load_real_ohlcv(symbol)
        if real_bars:
            for bar in real_bars[-RL_LOOKBACK:]:
                _price_bars[symbol].append(bar)
            print(f"[CH-RL] Seeded {symbol} with {len(_price_bars[symbol])} real OHLCV bars")
            return

        # Fallback: synthetic bars with small noise
        for i in range(RL_LOOKBACK):
            noise = random.uniform(-0.02, 0.02) * ref_price
            p = ref_price + noise
            _price_bars[symbol].append({
                "open": round(p, 2),
                "high": round(p + abs(noise) * 0.5, 2),
                "low": round(p - abs(noise) * 0.5, 2),
                "close": round(p, 2),
                "volume": random.randint(100, 500),
            })


def _finalize_bar(symbol: str):
    """Move current accumulator into the history deque. Must hold _bars_lock."""
    bar = _current_bar.pop(symbol, None)
    if not bar:
        return
    if symbol not in _price_bars:
        _price_bars[symbol] = deque(maxlen=120)
    _price_bars[symbol].append({
        "open": bar["open"], "high": bar["high"],
        "low": bar["low"], "close": bar["close"],
        "volume": bar["volume"],
    })


def get_bar_count(symbol: str) -> int:
    with _bars_lock:
        return len(_price_bars.get(symbol, []))


# ── Technical indicators ──────────────────────────────────────────────────────

def _compute_indicators(bars: list[dict]) -> np.ndarray:
    """Compute technical indicators from OHLCV bars. Returns (n_bars, n_features) array."""
    n = len(bars)
    close = np.array([b["close"] for b in bars], dtype=np.float64)
    high = np.array([b["high"] for b in bars], dtype=np.float64)
    low = np.array([b["low"] for b in bars], dtype=np.float64)
    opn = np.array([b["open"] for b in bars], dtype=np.float64)
    volume = np.array([b["volume"] for b in bars], dtype=np.float64)

    def sma(arr, w):
        out = np.full(n, np.nan)
        if n >= w:
            cs = np.cumsum(arr)
            out[w - 1:] = (cs[w - 1:] - np.concatenate([[0], cs[:-w]])) / w
        # Fill NaN with first valid
        for i in range(n):
            if np.isnan(out[i]):
                out[i] = arr[i]
            else:
                break
        out[:] = np.where(np.isnan(out), out[np.argmax(~np.isnan(out))], out)
        return out

    def ema(arr, span):
        out = np.empty(n)
        alpha = 2.0 / (span + 1)
        out[0] = arr[0]
        for i in range(1, n):
            out[i] = alpha * arr[i] + (1 - alpha) * out[i - 1]
        return out

    sma5 = sma(close, 5)
    sma10 = sma(close, 10)
    sma20 = sma(close, 20)
    sma50 = sma(close, 50)
    ema12 = ema(close, 12)
    ema26 = ema(close, 26)

    macd = ema12 - ema26
    signal = ema(macd, 9)
    histogram = macd - signal

    # RSI
    delta = np.diff(close, prepend=close[0])
    gain = np.where(delta > 0, delta, 0.0)
    loss = np.where(delta < 0, -delta, 0.0)
    avg_gain = ema(gain, 14)
    avg_loss = ema(loss, 14)
    rs = np.where(avg_loss > 0, avg_gain / avg_loss, 100.0)
    rsi = 100.0 - 100.0 / (1.0 + rs)

    # Bollinger Bands
    bb_mid = sma20
    bb_std = np.full(n, 0.01)
    for i in range(n):
        start = max(0, i - 19)
        bb_std[i] = max(np.std(close[start:i + 1]), 0.01)
    bb_upper = bb_mid + 2 * bb_std
    bb_lower = bb_mid - 2 * bb_std
    bb_width = (bb_upper - bb_lower) / np.where(bb_mid > 0, bb_mid, 1.0)
    bb_pos = (close - bb_lower) / np.where(bb_upper - bb_lower > 0, bb_upper - bb_lower, 1.0)

    # Volatility
    vol20 = np.full(n, 0.01)
    for i in range(n):
        start = max(0, i - 19)
        vol20[i] = max(np.std(close[start:i + 1]), 0.01)

    # Price changes
    price_change = np.diff(close, prepend=close[0]) / np.where(close > 0, close, 1.0)
    price_change_5 = np.zeros(n)
    for i in range(5, n):
        price_change_5[i] = (close[i] - close[i - 5]) / close[i - 5] if close[i - 5] > 0 else 0

    hl_ratio = high / np.where(low > 0, low, 1.0)
    oc_ratio = close / np.where(opn > 0, opn, 1.0)

    vol_sma = sma(volume, 20)
    vol_ratio = volume / np.where(vol_sma > 0, vol_sma, 1.0)

    # Base features (20 columns)
    features = np.column_stack([
        close, volume, sma5, sma10, sma20, sma50,
        ema12, ema26, rsi, macd, signal, histogram,
        bb_upper, bb_lower, bb_width, bb_pos,
        vol20, price_change, hl_ratio, vol_ratio,
    ])

    # Lagged features: close, volume, price_change, rsi, macd, vol20 at lags 1,2,3,5,10
    lag_sources = np.column_stack([close, volume, price_change, rsi, macd, vol20])
    lag_cols = []
    for lag in [1, 2, 3, 5, 10]:
        shifted = np.roll(lag_sources, lag, axis=0)
        shifted[:lag] = lag_sources[0]  # fill with first value
        lag_cols.append(shifted)
    lags = np.hstack(lag_cols)  # (n, 30)

    return np.hstack([features, lags])  # (n, 50)


# ── Observation builder ───────────────────────────────────────────────────────

def _build_observation(
    symbol: str,
    capital: float,
    holdings: list,
    bbos: dict,
) -> Optional[np.ndarray]:
    """Build the 3008-dim observation vector for one symbol."""
    with _bars_lock:
        bars = list(_price_bars.get(symbol, []))

    if len(bars) < RL_MIN_BARS:
        return None

    # Pad to RL_LOOKBACK if needed
    while len(bars) < RL_LOOKBACK:
        bars.insert(0, bars[0])

    bars = bars[-RL_LOOKBACK:]

    indicators = _compute_indicators(bars)  # (60, 50)

    # Scale using the loaded scaler
    if _scaler is not None:
        try:
            indicators = _scaler.transform(indicators)
        except Exception:
            # Shape mismatch — normalize manually
            mean = indicators.mean(axis=0)
            std = indicators.std(axis=0)
            std[std == 0] = 1.0
            indicators = (indicators - mean) / std

    market_state = indicators.flatten()  # 3000

    # Portfolio state (8 features)
    held_qty = 0
    held_value = 0.0
    for h in holdings:
        if h["symbol"] == symbol:
            held_qty = h["quantity"]
            bbo = bbos.get(symbol, {})
            price = bbo.get("best_bid") or h["avg_cost"]
            held_value = held_qty * price
            break

    net_worth = capital + sum(
        h["quantity"] * (bbos.get(h["symbol"], {}).get("best_bid") or h["avg_cost"])
        for h in holdings
    )
    returns = (net_worth - 100_000) / 100_000  # relative to starting capital

    portfolio_state = np.array([
        capital, held_qty, net_worth, returns,
        held_value, float(held_qty > 0),
        capital / max(net_worth, 1.0),
        held_value / max(net_worth, 1.0),
    ], dtype=np.float64)

    obs = np.concatenate([market_state, portfolio_state])
    return obs.astype(np.float32)


# ── Main decision function ────────────────────────────────────────────────────

def decide_order_rl(
    member_id: str,
    capital: float,
    holdings: list,
    bbos: dict,
    obligation_remaining: int,
) -> Optional[dict]:
    """Use the RL model to decide a trade. Returns order dict or None."""
    if not _model:
        if not _load_model():
            return None

    # Seed price history from BBOs for symbols we haven't seen
    for sym, bbo in bbos.items():
        mid = None
        if bbo.get("best_bid") and bbo.get("best_ask"):
            mid = (bbo["best_bid"] + bbo["best_ask"]) / 2
        elif bbo.get("best_bid"):
            mid = bbo["best_bid"]
        elif bbo.get("best_ask"):
            mid = bbo["best_ask"]
        if mid:
            seed_price(sym, mid)

    # Run prediction for each symbol, pick the best actionable one
    candidates = []

    for sym, bbo in bbos.items():
        obs = _build_observation(sym, capital, holdings, bbos)
        if obs is None:
            continue

        try:
            action, _ = _model.predict(obs, deterministic=False)
            action_type = int(round(float(action[0])))
            position_size = float(np.clip(action[1], 0.05, 0.5))

            action_type = max(0, min(2, action_type))

            if action_type == 0:
                continue  # Hold

            if action_type == 1:  # Buy
                ask = bbo.get("best_ask")
                if not ask or ask <= 0:
                    continue
                affordable = int(capital // ask)
                qty = max(10, int(affordable * position_size))
                qty = min(qty, 200)
                if qty * ask > capital:
                    qty = int(capital // ask)
                if qty < 10:
                    continue
                candidates.append({
                    "symbol": sym, "side": "BUY",
                    "quantity": qty, "price": round(ask, 2),
                    "score": position_size,
                })

            elif action_type == 2:  # Sell
                held = next((h["quantity"] for h in holdings if h["symbol"] == sym), 0)
                if held <= 0:
                    continue
                bid = bbo.get("best_bid")
                if not bid or bid <= 0:
                    continue
                qty = max(10, int(held * position_size))
                qty = min(qty, held, 200)
                if qty < 10:
                    continue
                candidates.append({
                    "symbol": sym, "side": "SELL",
                    "quantity": qty, "price": round(bid, 2),
                    "score": position_size,
                })
        except Exception as e:
            print(f"[CH-RL] Prediction error for {sym}: {e}")
            continue

    if not candidates:
        # If obligation remains, force a buy on a random affordable symbol
        if obligation_remaining > 0:
            affordable = [
                (sym, bbo) for sym, bbo in bbos.items()
                if bbo.get("best_ask") and 10 * bbo["best_ask"] <= capital
            ]
            if affordable:
                sym, bbo = random.choice(affordable)
                return {
                    "symbol": sym, "side": "BUY",
                    "quantity": random.randint(10, 50),
                    "price": round(bbo["best_ask"], 2),
                }
        return None

    # Pick highest-confidence candidate
    candidates.sort(key=lambda c: c["score"], reverse=True)
    best = candidates[0]
    best.pop("score")
    return best
