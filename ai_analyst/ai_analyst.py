import sys
sys.path.insert(0, "/app")

import threading, time, os, json, datetime, requests
from collections import deque

from shared.config import Config
from shared.kafka_utils import create_producer, create_consumer

# ── Config ─────────────────────────────────────────────────────────────────────
OLLAMA_HOST    = os.getenv("OLLAMA_HOST", "")          # e.g. http://host.docker.internal:11434
OLLAMA_MODEL   = os.getenv("OLLAMA_MODEL", "llama3.1:8b")
HF_TOKEN       = os.getenv("HF_TOKEN", "")
HF_MODEL       = os.getenv("HF_MODEL", "Qwen/Qwen2.5-7B-Instruct-1M")
ANALYSIS_INTERVAL = int(os.getenv("ANALYSIS_INTERVAL", "1800"))  # 30 min default

# ── Rolling market data buffers ────────────────────────────────────────────────
recent_trades     = deque(maxlen=200)
latest_snapshots  = {}          # symbol -> snapshot dict
lock              = threading.Lock()

_running   = False
_suspended = False

# ── LLM call ──────────────────────────────────────────────────────────────────

def call_llm(prompt: str) -> str | None:
    """Try Ollama first, fall back to HuggingFace Inference API."""

    # 1. Ollama (local)
    if OLLAMA_HOST:
        try:
            resp = requests.post(
                f"{OLLAMA_HOST}/api/chat",
                json={
                    "model":    OLLAMA_MODEL,
                    "messages": [{"role": "user", "content": prompt}],
                    "stream":   False,
                },
                timeout=90,
            )
            if resp.status_code == 200:
                text = resp.json().get("message", {}).get("content", "").strip()
                if text:
                    print(f"[AI-Analyst] Insight via Ollama ({OLLAMA_MODEL})")
                    return text
            else:
                print(f"[AI-Analyst] Ollama HTTP {resp.status_code}: {resp.text[:200]}")
        except Exception as e:
            print(f"[AI-Analyst] Ollama unreachable: {e}")

    # 2. HuggingFace Inference API — router.huggingface.co (OpenAI-compatible)
    if HF_TOKEN:
        url = "https://router.huggingface.co/v1/chat/completions"
        print(f"[AI-Analyst] Calling HF router: model={HF_MODEL}")
        for attempt in range(3):
            try:
                resp = requests.post(
                    url,
                    headers={
                        "Authorization": f"Bearer {HF_TOKEN}",
                        "Content-Type":  "application/json",
                    },
                    json={
                        "model":       HF_MODEL,
                        "messages":    [{"role": "user", "content": prompt}],
                        "max_tokens":  220,
                        "temperature": 0.7,
                    },
                    timeout=60,
                )
                print(f"[AI-Analyst] HF response status: {resp.status_code}")
                if resp.status_code == 200:
                    text = resp.json()["choices"][0]["message"]["content"].strip()
                    if text:
                        print(f"[AI-Analyst] Insight via HuggingFace ({HF_MODEL})")
                        return text
                elif resp.status_code == 503:
                    body = resp.json() if resp.content else {}
                    wait = body.get("estimated_time", 20)
                    print(f"[AI-Analyst] HF model loading, waiting {wait:.0f}s (attempt {attempt+1}/3)")
                    time.sleep(min(float(wait), 30))
                else:
                    print(f"[AI-Analyst] HF HTTP {resp.status_code}: {resp.text[:400]}")
                    break
            except Exception as e:
                print(f"[AI-Analyst] HF API error (attempt {attempt+1}/3): {e}")
                break

    return None


# ── Prompt builder ─────────────────────────────────────────────────────────────

def build_prompt() -> str:
    now     = datetime.datetime.now().strftime("%H:%M:%S")
    cutoff  = time.time() - ANALYSIS_INTERVAL

    with lock:
        trades_snap = list(recent_trades)
        snaps_snap  = dict(latest_snapshots)
        session_str = ("ACTIVE" if _running and not _suspended
                       else "SUSPENDED" if _suspended else "IDLE")

    # Recent trades summary per symbol
    recent = [t for t in trades_snap if float(t.get("timestamp", 0)) >= cutoff]
    interval_label = f"{ANALYSIS_INTERVAL // 60} min" if ANALYSIS_INTERVAL >= 60 else f"{ANALYSIS_INTERVAL}s"
    if recent:
        by_sym: dict = {}
        for t in recent:
            sym = t.get("symbol", "?")
            by_sym.setdefault(sym, []).append(t)
        trade_lines = []
        for sym, ts in sorted(by_sym.items()):
            prices = [float(t.get("price", 0)) for t in ts]
            vol    = sum(int(t.get("quantity") or t.get("qty") or 0) for t in ts)
            trade_lines.append(
                f"  {sym}: {len(ts)} trade(s), "
                f"range {min(prices):.2f}–{max(prices):.2f}, "
                f"vol {vol}, last {prices[-1]:.2f}"
            )
        trades_block = "\n".join(trade_lines)
    else:
        trades_block = "  No trades in the last interval"

    # Order book snapshot
    if snaps_snap:
        book_lines = []
        for sym, snap in sorted(snaps_snap.items()):
            bid = snap.get("best_bid")
            ask = snap.get("best_ask")
            if bid and ask:
                spread = float(ask) - float(bid)
                book_lines.append(f"  {sym}: Bid {bid} / Ask {ask} (spread {spread:.2f})")
            else:
                book_lines.append(f"  {sym}: Bid {bid or '-'} / Ask {ask or '-'}")
        book_block = "\n".join(book_lines)
    else:
        book_block = "  No order book data yet"

    return f"""You are a concise financial market analyst for a simulated stock exchange.
Time: {now} | Session: {session_str}

Trades in the last {interval_label}:
{trades_block}

Order book (best bid/ask):
{book_block}

In 3–4 sentences analyse: activity level, notable price moves or volume spikes, market sentiment.
Be specific and data-driven. No headers, no bullet points, plain prose only."""


def run_immediate_analysis(producer):
    """Called on-demand (button click). Skips the interval check."""
    print("[AI-Analyst] On-demand analysis triggered")
    prompt = build_prompt()
    text   = call_llm(prompt)
    if text:
        insight = {"text": text, "timestamp": time.time()}
        producer.send(Config.AI_INSIGHTS_TOPIC, insight)
        producer.flush()
        print(f"[AI-Analyst] On-demand insight published ({len(text)} chars)")


# ── Kafka consumer (market data) ──────────────────────────────────────────────

def consume_market_data(producer):
    global _running, _suspended
    consumer = create_consumer(
        topics=[
            Config.TRADES_TOPIC,
            Config.SNAPSHOTS_TOPIC,
            Config.CONTROL_TOPIC,
        ],
        group_id="ai-analyst",
        component_name="AI-Analyst",
        auto_offset_reset="latest",
    )
    for msg in consumer:
        with lock:
            if msg.topic == Config.TRADES_TOPIC:
                recent_trades.append(msg.value)
            elif msg.topic == Config.SNAPSHOTS_TOPIC:
                snap = msg.value
                sym  = snap.get("symbol")
                if sym:
                    latest_snapshots[sym] = snap
            elif msg.topic == Config.CONTROL_TOPIC:
                action = msg.value.get("action", "")
                if action == "start":
                    _running   = True
                    _suspended = False
                elif action in ("end", "stop"):
                    _running   = False
                elif action == "suspend":
                    _suspended = True
                elif action == "resume":
                    _suspended = False
                elif action == "generate_insight":
                    threading.Thread(target=run_immediate_analysis, args=(producer,), daemon=True).start()


# ── Analysis loop ──────────────────────────────────────────────────────────────

def analysis_loop(producer):
    print(f"[AI-Analyst] Analysis loop started (interval={ANALYSIS_INTERVAL}s)")
    if OLLAMA_HOST:
        print(f"[AI-Analyst] Ollama: {OLLAMA_HOST}  model: {OLLAMA_MODEL}")
    if HF_TOKEN:
        print(f"[AI-Analyst] HuggingFace fallback: model={HF_MODEL}")
    if not OLLAMA_HOST and not HF_TOKEN:
        print("[AI-Analyst] WARNING: neither OLLAMA_HOST nor HF_TOKEN configured — no insights will be generated")

    while True:
        time.sleep(ANALYSIS_INTERVAL)

        with lock:
            active = _running and not _suspended

        if not active:
            continue

        prompt = build_prompt()
        text   = call_llm(prompt)

        if text:
            insight = {"text": text, "timestamp": time.time()}
            producer.send(Config.AI_INSIGHTS_TOPIC, insight)
            producer.flush()
            print(f"[AI-Analyst] Published insight ({len(text)} chars)")


# ── Entry point ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    producer = create_producer(component_name="AI-Analyst")
    threading.Thread(target=consume_market_data, args=(producer,), daemon=True).start()
    analysis_loop(producer)
