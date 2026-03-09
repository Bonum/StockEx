"""AI-driven simulation of CH members not currently controlled by a real user.

Three background threads:
  1. _trade_consumer_thread  – Kafka consumer on 'trades' topic; attributes
     trades back to CH members whose cl_ord_id starts with USRxx-.
  2. _simulation_thread      – every CH_AI_INTERVAL seconds, picks an order
     for each unoccupied member using the configured strategy.
  3. _control_listener_thread – listens for session start/stop/suspend/resume.

Strategy is selected via CH_AI_STRATEGY env var:
  "hybrid"     – USR01-04 LLM, USR05-07 NN1, USR08-10 NN2 (default)
  "hybrid-nn1" – USR01-05 NN1, USR06-10 LLM
  "hybrid-nn2" – USR01-05 NN2, USR06-10 LLM
  "llm"        – all members use LLM
  "nn1"        – all members use NN1 (Adilbai/stock-trading-rl-agent)
  "nn2"        – all members use NN2 (RayMelius/stockex-nn-agent)
  Legacy alias: "rl" → "nn1"

Call start() once from app.py after init_db().
Call set_human_active(member_id) / set_human_inactive(member_id) on login/logout.
"""

import sys
sys.path.insert(0, "/app")

import json
import os
import random
import re
import threading
import time
from typing import Optional

import requests

from shared.config import Config
from shared.kafka_utils import create_consumer, create_producer

import ch_database as db

# RL trader (optional – graceful fallback if deps not installed)
try:
    import ch_rl_trader as rl_trader
    _rl_available = True
except ImportError:
    _rl_available = False
    print("[CH-AI] RL dependencies not installed — RL strategy unavailable")

# ── Config ─────────────────────────────────────────────────────────────────────
CH_AI_INTERVAL = int(os.getenv("CH_AI_INTERVAL", "45"))   # seconds between AI cycles
# Normalize legacy strategy names on startup
_raw_strategy = os.getenv("CH_AI_STRATEGY", "hybrid")
_STRATEGY_ALIASES = {"rl": "nn1", "hybrid-nn1": "hybrid-nn1", "hybrid-nn2": "hybrid-nn2"}
CH_AI_STRATEGY = _STRATEGY_ALIASES.get(_raw_strategy, _raw_strategy)
VALID_STRATEGIES = {"llm", "nn1", "nn2", "hybrid", "hybrid-nn1", "hybrid-nn2"}
CH_SOURCE      = "CLRH"

OLLAMA_HOST  = os.getenv("OLLAMA_HOST", "")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.1:8b")
HF_TOKEN     = os.getenv("HF_TOKEN", "")
HF_MODEL     = os.getenv("CH_HF_MODEL", os.getenv("HF_MODEL", "RayMelius/stockex-ch-trader"))
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL   = os.getenv("GROQ_MODEL", "llama-3.1-8b-instant")
GROQ_URL     = "https://api.groq.com/openai/v1/chat/completions"

# ── Shared state ───────────────────────────────────────────────────────────────
_active_humans: set[str] = set()   # member_ids currently logged-in as humans
_humans_lock = threading.Lock()

_running   = False
_suspended = False

_order_seq = 0
_seq_lock  = threading.Lock()

_producer = None
_producer_lock = threading.Lock()


# ── Public API ─────────────────────────────────────────────────────────────────

def set_human_active(member_id: str) -> None:
    with _humans_lock:
        _active_humans.add(member_id)


def set_human_inactive(member_id: str) -> None:
    with _humans_lock:
        _active_humans.discard(member_id)


def is_human_active(member_id: str) -> bool:
    with _humans_lock:
        return member_id in _active_humans


def get_strategy() -> str:
    return CH_AI_STRATEGY


def set_strategy(strategy: str) -> str:
    """Dynamically switch AI strategy. Returns the active strategy."""
    global CH_AI_STRATEGY
    strategy = strategy.lower().strip()
    # Support legacy aliases
    strategy = _STRATEGY_ALIASES.get(strategy, strategy)
    if strategy not in VALID_STRATEGIES:
        return CH_AI_STRATEGY
    if strategy != "llm" and not _rl_available:
        print(f"[CH-AI] Cannot switch to {strategy}: RL deps not installed")
        return CH_AI_STRATEGY
    CH_AI_STRATEGY = strategy
    # Tell RL trader which model slot to use
    if _rl_available and strategy in ("nn1", "nn2"):
        rl_trader.set_active_model(strategy)
    elif _rl_available and strategy.startswith("hybrid-"):
        nn_slot = strategy.split("-", 1)[1]
        rl_trader.set_active_model(nn_slot)
    # "hybrid" uses both nn1 and nn2, no single active model to set
    print(f"[CH-AI] Strategy switched to: {strategy}")
    return CH_AI_STRATEGY


def start() -> None:
    """Start background threads. Call once after app startup."""
    global _running
    _running = True
    threading.Thread(target=_trade_consumer_thread, daemon=True, name="ch-trade-consumer").start()
    threading.Thread(target=_simulation_thread,     daemon=True, name="ch-ai-sim").start()
    threading.Thread(target=_control_listener_thread, daemon=True, name="ch-control").start()
    strategy = CH_AI_STRATEGY
    if strategy != "llm" and not _rl_available:
        strategy = "llm"
        print("[CH-AI] WARNING: NN requested but deps missing, falling back to LLM")
    print(f"[CH-AI] Background threads started (strategy={strategy})")


# ── Kafka helpers ──────────────────────────────────────────────────────────────

def _get_producer():
    global _producer
    with _producer_lock:
        if _producer is None:
            _producer = create_producer(component_name="CH-AI")
    return _producer


def _next_cl_ord_id(member_id: str) -> str:
    global _order_seq
    with _seq_lock:
        _order_seq += 1
        return f"{member_id}-{int(time.time() * 1000)}-{_order_seq}"


# ── Thread 1: Trade consumer (attribution) ─────────────────────────────────────

def _trade_consumer_thread():
    """Consume 'trades' topic and attribute CH member trades to their accounts."""
    try:
        consumer = create_consumer(
            Config.TRADES_TOPIC,
            group_id="clearing-house-trades",
            auto_offset_reset="latest",
            component_name="CH-TradeConsumer",
        )
    except Exception as e:
        print(f"[CH-AI] Trade consumer failed to start: {e}")
        return

    for msg in consumer:
        if not _running:
            break
        try:
            trade = msg.value
            buy_id  = trade.get("buy_id") or trade.get("buy_order_id") or ""
            sell_id = trade.get("sell_id") or trade.get("sell_order_id") or ""
            symbol  = trade.get("symbol", "")
            price   = float(trade.get("price", 0))
            qty     = int(trade.get("quantity", 0))

            if not symbol or price <= 0 or qty <= 0:
                continue

            # Feed every trade into RL price history (regardless of strategy)
            if _rl_available:
                try:
                    rl_trader.feed_trade(symbol, price, qty)
                except Exception:
                    pass

            # Detect CH member orders by cl_ord_id prefix pattern USRxx-
            for order_id, side in [(buy_id, "BUY"), (sell_id, "SELL")]:
                m = re.match(r"^(USR\d{2})-", order_id)
                if m:
                    member_id = m.group(1)
                    db.record_trade(member_id, symbol, side, qty, price, order_id)
                    print(f"[CH-AI] Attributed {side} {qty} {symbol}@{price:.2f} → {member_id}")
        except Exception as e:
            print(f"[CH-AI] Trade attribution error: {e}")


# ── Thread 3: Control listener ─────────────────────────────────────────────────

def _control_listener_thread():
    global _running, _suspended
    try:
        consumer = create_consumer(
            Config.CONTROL_TOPIC,
            group_id="clearing-house-control",
            auto_offset_reset="latest",
            component_name="CH-Control",
        )
    except Exception as e:
        print(f"[CH-AI] Control consumer failed: {e}")
        return

    for msg in consumer:
        try:
            action = msg.value.get("action", "")
            if action in ("stop", "end"):
                _suspended = True
                print("[CH-AI] Session stopped — AI simulation paused")
            elif action == "start":
                _suspended = False
                print("[CH-AI] Session started — AI simulation active")
            elif action == "suspend":
                _suspended = True
            elif action == "resume":
                _suspended = False
        except Exception as e:
            print(f"[CH-AI] Control error: {e}")


# ── Thread 2: AI simulation ────────────────────────────────────────────────────

def _simulation_thread():
    """Every CH_AI_INTERVAL seconds, generate a trade for each unoccupied member."""
    while _running:
        time.sleep(CH_AI_INTERVAL)
        if _suspended:
            continue
        try:
            _run_simulation_cycle()
        except Exception as e:
            print(f"[CH-AI] Simulation cycle error: {e}")


def _run_simulation_cycle():
    # Fetch current BBO from Matcher
    bbos = _fetch_bbos()
    if not bbos:
        return

    today = db.today_str()
    members = db.get_all_members()

    for member in members:
        mid = member["member_id"]
        if is_human_active(mid):
            continue

        dt = db.get_daily_trades(mid, today)
        obligation_remaining = max(0, db.CH_DAILY_OBLIGATION - dt["total_securities"])
        holdings = db.get_holdings(mid)
        capital = member["capital"]

        # Skip if no market data and obligation already met
        if obligation_remaining == 0 and random.random() > 0.3:
            continue  # occasionally trade even after obligation met

        order = _decide_order(mid, capital, holdings, dt, bbos, obligation_remaining)
        if order:
            _submit_order(mid, order)
            time.sleep(0.5)  # stagger submissions


def _decide_order(member_id, capital, holdings, daily_trades, bbos, obligation_remaining):
    """Dispatch to the configured strategy."""
    strategy = CH_AI_STRATEGY

    # Hybrid modes: split members between strategies
    member_num = int(member_id[-2:])
    if strategy == "hybrid" and _rl_available:
        # Default hybrid: USR01-04 LLM, USR05-07 NN1, USR08-10 NN2
        if member_num <= 4:
            strategy = "llm"
        elif member_num <= 7:
            strategy = "nn1"
        else:
            strategy = "nn2"
    elif strategy.startswith("hybrid-") and _rl_available:
        nn_slot = strategy.split("-", 1)[1]  # "nn1" or "nn2"
        if member_num <= 5:
            strategy = nn_slot
        else:
            strategy = "llm"

    if strategy in ("nn1", "nn2") and _rl_available:
        try:
            order = rl_trader.decide_order_rl(
                member_id, capital, holdings, bbos, obligation_remaining,
                model_slot=strategy,
            )
            if order and _validate_order(order, capital, holdings, bbos):
                try:
                    db.record_ai_decision(member_id, f"RL({strategy}): {order}", order, source=strategy)
                except Exception:
                    pass
                return order
        except Exception as e:
            print(f"[CH-AI] {strategy} strategy error for {member_id}: {e}")
        # Fall through to LLM on NN failure
        return _decide_order_llm(
            member_id, capital, holdings, daily_trades, bbos, obligation_remaining,
        )

    return _decide_order_llm(
        member_id, capital, holdings, daily_trades, bbos, obligation_remaining,
    )


def _load_reference_prices() -> dict:
    """Load reference prices from securities.txt as fallback when books are empty."""
    ref = {}
    secs_file = os.getenv("SECURITIES_FILE", "/app/shared/data/securities.txt")
    try:
        with open(secs_file) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    parts = line.split()
                    if len(parts) >= 3:
                        sym, _, current = parts[0], float(parts[1]), float(parts[2])
                        ref[sym] = current
                    elif len(parts) >= 2:
                        ref[parts[0]] = float(parts[1])
    except Exception:
        pass
    return ref


def _fetch_bbos() -> dict:
    """Get BBO for all symbols from Matcher API, falling back to reference prices."""
    try:
        secs_file = os.getenv("SECURITIES_FILE", "/app/shared/data/securities.txt")
        symbols = []
        try:
            with open(secs_file) as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#"):
                        parts = line.split()
                        if parts:
                            symbols.append(parts[0])
        except Exception:
            pass

        if not symbols:
            return {}

        bbos = {}
        matcher_url = os.getenv("MATCHER_URL", Config.MATCHER_URL)
        for sym in symbols:
            try:
                r = requests.get(f"{matcher_url}/orderbook/{sym}", timeout=2)
                if r.status_code == 200:
                    book = r.json()
                    bids = book.get("bids", [])
                    asks = book.get("asks", [])
                    best_bid = max((b["price"] for b in bids), default=None)
                    best_ask = min((a["price"] for a in asks), default=None)
                    if best_bid or best_ask:
                        bbos[sym] = {"best_bid": best_bid, "best_ask": best_ask}
            except Exception:
                pass

        # Fall back to reference prices for symbols with no live BBO
        if len(bbos) < len(symbols):
            ref_prices = _load_reference_prices()
            spread = 0.10
            for sym in symbols:
                if sym not in bbos and sym in ref_prices:
                    mid = ref_prices[sym]
                    bbos[sym] = {
                        "best_bid": round(mid - spread, 2),
                        "best_ask": round(mid + spread, 2),
                    }
            if ref_prices:
                print(f"[CH-AI] Using reference prices for {len(bbos)} symbols "
                      f"({len(bbos) - len([s for s in bbos if bbos[s].get('best_bid')])} from file)")

        return bbos
    except Exception as e:
        print(f"[CH-AI] BBO fetch error: {e}")
        return {}


def _decide_order_llm(
    member_id: str,
    capital: float,
    holdings: list,
    daily_trades: dict,
    bbos: dict,
    obligation_remaining: int,
) -> Optional[dict]:
    """Call LLM to decide the next trade. Falls back to rule-based on failure."""
    prompt = _build_prompt(member_id, capital, holdings, daily_trades, bbos, obligation_remaining)
    text = _call_llm(prompt)
    if text:
        order = _parse_llm_order(text, bbos)
        if order and _validate_order(order, capital, holdings, bbos):
            try:
                db.record_ai_decision(member_id, text, order, source="llm")
            except Exception as e:
                print(f"[CH-AI] Failed to log decision: {e}")
            return order
        # Log failed parse/validation too
        try:
            db.record_ai_decision(member_id, text, order, source="llm-invalid")
        except Exception:
            pass
    # Fallback: rule-based
    fallback = _fallback_order(capital, holdings, bbos)
    if fallback:
        try:
            db.record_ai_decision(member_id, "LLM unavailable, using rule-based fallback", fallback, source="fallback")
        except Exception:
            pass
    return fallback


def _build_prompt(member_id, capital, holdings, daily_trades, bbos, obligation_remaining):
    market_lines = []
    for sym, bbo in sorted(bbos.items()):
        bid = f"{bbo['best_bid']:.2f}" if bbo.get("best_bid") else "-"
        ask = f"{bbo['best_ask']:.2f}" if bbo.get("best_ask") else "-"
        market_lines.append(f"  {sym}: Bid {bid} / Ask {ask}")

    holding_lines = [
        f"  {h['symbol']}: {h['quantity']} shares @ avg cost {h['avg_cost']:.2f}"
        for h in holdings
    ] if holdings else ["  None"]

    return (
        f"You are simulating clearing house member {member_id} making ONE trading decision.\n\n"
        f"Member state:\n"
        f"  Available capital: EUR {capital:,.2f}\n"
        f"  Securities obligation remaining today: {obligation_remaining} more to trade\n"
        f"  Current holdings:\n" + "\n".join(holding_lines) + "\n\n"
        f"Current market (Bid/Ask):\n" + "\n".join(market_lines) + "\n\n"
        f"Rules:\n"
        f"- Do not spend more than your available capital\n"
        f"- Do not sell more shares than you hold\n"
        f"- If you have no holdings, you must BUY\n"
        f"- Choose a realistic price close to the BBO mid-price\n"
        f"- Quantity should be between 10 and 200\n\n"
        f"Respond ONLY with valid JSON, no other text:\n"
        f'Example: {{"symbol": "ALPHA", "side": "BUY", "quantity": 50, "price": 5.95}}'
    )


def _parse_llm_order(text: str, bbos: dict) -> Optional[dict]:
    try:
        match = re.search(r"\{[^}]+\}", text, re.DOTALL)
        if not match:
            return None
        data = json.loads(match.group())
        return {
            "symbol":   str(data.get("symbol", "")).upper(),
            "side":     str(data.get("side", "")).upper(),
            "quantity": int(data.get("quantity", 0)),
            "price":    float(data.get("price", 0)),
        }
    except Exception:
        return None


def _validate_order(order: dict, capital: float, holdings: list, bbos: dict) -> bool:
    sym = order.get("symbol", "")
    side = order.get("side", "")
    qty = order.get("quantity", 0)
    price = order.get("price", 0)

    if sym not in bbos or side not in ("BUY", "SELL") or qty <= 0 or price <= 0:
        return False
    if side == "BUY" and qty * price > capital:
        return False
    if side == "SELL":
        held = next((h["quantity"] for h in holdings if h["symbol"] == sym), 0)
        if qty > held:
            return False
    return True


def _fallback_order(capital: float, holdings: list, bbos: dict) -> Optional[dict]:
    """Rule-based fallback: prefer BUY if no holdings, SELL if heavily loaded."""
    if not bbos:
        return None

    # Decide side based on portfolio balance
    total_holding_value = sum(
        h["quantity"] * (bbos.get(h["symbol"], {}).get("best_ask") or h["avg_cost"])
        for h in holdings
    )
    net_worth = capital + total_holding_value
    holdings_ratio = total_holding_value / net_worth if net_worth > 0 else 0

    if not holdings:
        side = "BUY"
    elif holdings_ratio > 0.6:
        side = random.choices(["SELL", "BUY"], weights=[0.7, 0.3])[0]
    elif holdings_ratio < 0.2:
        side = random.choices(["BUY", "SELL"], weights=[0.8, 0.2])[0]
    else:
        side = random.choices(["BUY", "SELL"], weights=[0.5, 0.5])[0]

    if side == "SELL" and not holdings:
        side = "BUY"

    if side == "BUY":
        # Pick a random affordable symbol
        affordable = [
            sym for sym, bbo in bbos.items()
            if bbo.get("best_ask") and 10 * bbo["best_ask"] <= capital
        ]
        if not affordable:
            return None
        sym = random.choice(affordable)
        ask = bbos[sym]["best_ask"]
        qty = min(random.randint(10, 100), int(capital // ask))
        if qty <= 0:
            return None
        return {"symbol": sym, "side": "BUY", "quantity": qty, "price": round(ask, 2)}
    else:
        # Sell from existing holdings
        h = random.choice(holdings)
        sym = h["symbol"]
        bbo = bbos.get(sym, {})
        bid = bbo.get("best_bid") or h["avg_cost"]
        qty = random.randint(10, max(10, h["quantity"] // 2))
        qty = min(qty, h["quantity"])
        if qty <= 0:
            return None
        return {"symbol": sym, "side": "SELL", "quantity": qty, "price": round(bid, 2)}


def _submit_order(member_id: str, order: dict) -> None:
    cl_ord_id = _next_cl_ord_id(member_id)
    msg = {
        "cl_ord_id":    cl_ord_id,
        "symbol":       order["symbol"],
        "side":         order["side"],
        "quantity":     order["quantity"],
        "price":        order["price"],
        "ord_type":     "LIMIT",
        "time_in_force": "DAY",
        "timestamp":    time.time(),
        "source":       CH_SOURCE,
    }
    try:
        _get_producer().send(Config.ORDERS_TOPIC, msg)
        print(f"[CH-AI] {member_id} → {order['side']} {order['quantity']} {order['symbol']}@{order['price']:.2f}")
    except Exception as e:
        print(f"[CH-AI] Order submit failed: {e}")


# ── LLM (Groq → HF → Ollama fallback) ────────────────────────────────────────

def _call_llm(prompt: str) -> Optional[str]:
    if os.getenv("SPACE_ID"):
        # Running on HuggingFace Spaces — prefer Groq (free), skip custom HF model
        return _try_groq(prompt) or _try_ollama(prompt)
    # Local / self-hosted — prefer fine-tuned model via Ollama
    return _try_ollama(prompt) or _try_groq(prompt) or _try_hf(prompt)


def _try_groq(prompt: str) -> Optional[str]:
    if not GROQ_API_KEY:
        return None
    try:
        resp = requests.post(
            GROQ_URL,
            headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
            json={
                "model": GROQ_MODEL,
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 100,
                "temperature": 0.4,
            },
            timeout=20,
        )
        if resp.status_code == 200:
            return resp.json()["choices"][0]["message"]["content"].strip()
    except Exception as e:
        print(f"[CH-AI] Groq error: {e}")
    return None


def _try_hf(prompt: str) -> Optional[str]:
    if not HF_TOKEN:
        return None
    url = "https://router.huggingface.co/v1/chat/completions"
    try:
        resp = requests.post(
            url,
            headers={"Authorization": f"Bearer {HF_TOKEN}", "Content-Type": "application/json"},
            json={
                "model": HF_MODEL,
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 100,
                "temperature": 0.4,
            },
            timeout=30,
        )
        if resp.status_code == 200:
            return resp.json()["choices"][0]["message"]["content"].strip()
    except Exception as e:
        print(f"[CH-AI] HF error: {e}")
    return None


def _try_ollama(prompt: str) -> Optional[str]:
    if not OLLAMA_HOST:
        return None
    try:
        resp = requests.post(
            f"{OLLAMA_HOST}/api/chat",
            json={"model": OLLAMA_MODEL, "messages": [{"role": "user", "content": prompt}], "stream": False},
            timeout=60,
        )
        if resp.status_code == 200:
            return resp.json().get("message", {}).get("content", "").strip()
    except Exception as e:
        print(f"[CH-AI] Ollama error: {e}")
    return None
