import sys
sys.path.insert(0, "/app")

from flask import Flask, render_template, jsonify, Response, request
import threading, json, os, time, requests, sqlite3, datetime
from queue import Queue, Empty

from shared.config import Config
from shared.kafka_utils import create_consumer, create_producer

app = Flask(__name__, template_folder="templates")
app.config["TEMPLATES_AUTO_RELOAD"] = True

# Shared state
orders, bbos, trades_cache = [], {}, []
ai_insights_cache = []
lock = threading.Lock()

# SSE: list of queues for connected clients
sse_clients = []
sse_clients_lock = threading.Lock()

# Session state
session_state = {"active": False, "start_time": None, "suspended": False, "mode": "automatic"}

SCHEDULE_FILE  = os.getenv("SCHEDULE_FILE",  "/app/data/market_schedule.txt")
FRONTEND_URL   = os.getenv("FRONTEND_URL",   "")

# ── AI Analyst (inline LLM for on-demand generation) ───────────────────────────
HF_TOKEN  = os.getenv("HF_TOKEN", "")
HF_MODEL  = os.getenv("HF_MODEL", "Qwen/Qwen2.5-3B-Instruct")
HF_URL    = "https://router.huggingface.co/v1/chat/completions"
OLLAMA_HOST  = os.getenv("OLLAMA_HOST", "")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.1:8b")


def _build_market_prompt():
    with lock:
        snaps  = dict(bbos)
        recent = list(trades_cache[:30])
    now = datetime.datetime.now().strftime("%H:%M:%S")
    sess = ("ACTIVE" if session_state["active"] and not session_state["suspended"]
            else "SUSPENDED" if session_state["suspended"] else "IDLE")

    if recent:
        by_sym = {}
        for t in recent:
            by_sym.setdefault(t.get("symbol", "?"), []).append(t)
        trade_lines = []
        for sym, ts in sorted(by_sym.items()):
            prices = [float(t.get("price", 0)) for t in ts]
            vol    = sum(int(t.get("quantity") or t.get("qty") or 0) for t in ts)
            trade_lines.append(f"  {sym}: {len(ts)} trade(s), range {min(prices):.2f}–{max(prices):.2f}, vol {vol}, last {prices[-1]:.2f}")
        trades_block = "\n".join(trade_lines)
    else:
        trades_block = "  No recent trades"

    if snaps:
        book_lines = []
        for sym, s in sorted(snaps.items()):
            bid, ask = s.get("best_bid"), s.get("best_ask")
            spread = f"{float(ask)-float(bid):.2f}" if bid and ask else "?"
            book_lines.append(f"  {sym}: Bid {bid or '-'} / Ask {ask or '-'} (spread {spread})")
        book_block = "\n".join(book_lines)
    else:
        book_block = "  No order book data"

    return (f"You are a concise financial market analyst for a simulated stock exchange.\n"
            f"Time: {now} | Session: {sess}\n\n"
            f"Recent trades:\n{trades_block}\n\n"
            f"Order book:\n{book_block}\n\n"
            f"In 3-4 sentences: activity level, notable moves, market sentiment. "
            f"Plain prose, no headers, no bullet points.")


def _call_llm(prompt):
    """Try Ollama first, then HuggingFace router. Returns (text, source) or (None, error_msg)."""
    # 1. Ollama
    if OLLAMA_HOST:
        try:
            r = requests.post(f"{OLLAMA_HOST}/api/chat",
                              json={"model": OLLAMA_MODEL,
                                    "messages": [{"role": "user", "content": prompt}],
                                    "stream": False},
                              timeout=90)
            if r.status_code == 200:
                text = r.json().get("message", {}).get("content", "").strip()
                if text:
                    return text, "Ollama"
            print(f"[Dashboard/LLM] Ollama {r.status_code}: {r.text[:200]}")
        except Exception as e:
            print(f"[Dashboard/LLM] Ollama error: {e}")

    # 2. HuggingFace router
    if not HF_TOKEN:
        return None, "HF_TOKEN not set"
    print(f"[Dashboard/LLM] Calling HF router ({HF_MODEL})…")
    for attempt in range(3):
        try:
            r = requests.post(HF_URL,
                              headers={"Authorization": f"Bearer {HF_TOKEN}",
                                       "Content-Type": "application/json"},
                              json={"model": HF_MODEL,
                                    "messages": [{"role": "user", "content": prompt}],
                                    "max_tokens": 180,
                                    "temperature": 0.7},
                              timeout=90)
            print(f"[Dashboard/LLM] HF status {r.status_code} (attempt {attempt+1})")
            if r.status_code == 200:
                text = r.json()["choices"][0]["message"]["content"].strip()
                if text:
                    return text, HF_MODEL
            elif r.status_code == 503:
                body = {}
                try: body = r.json()
                except: pass
                wait = min(float(body.get("estimated_time", 20)), 30)
                print(f"[Dashboard/LLM] Model loading, waiting {wait:.0f}s…")
                time.sleep(wait)
            else:
                print(f"[Dashboard/LLM] HF error body: {r.text[:400]}")
                return None, f"HF HTTP {r.status_code}: {r.text[:120]}"
        except requests.exceptions.Timeout:
            print(f"[Dashboard/LLM] HF timeout (attempt {attempt+1})")
            return None, "HF request timed out after 90s"
        except Exception as e:
            print(f"[Dashboard/LLM] HF exception: {e}")
            return None, str(e)
    return None, "HF: max retries exceeded"


def _generate_and_broadcast():
    """Background thread: call LLM, publish result via SSE + Kafka."""
    prompt = _build_market_prompt()
    text, source = _call_llm(prompt)
    if text:
        insight = {"text": text, "source": source, "timestamp": time.time()}
        with lock:
            ai_insights_cache.insert(0, insight)
            ai_insights_cache[:] = ai_insights_cache[:10]
        broadcast_event("ai_insight", insight)
        try:
            get_producer().send(Config.AI_INSIGHTS_TOPIC, insight)
        except Exception:
            pass
        print(f"[Dashboard/LLM] Insight published ({len(text)} chars, src={source})")
    else:
        # Surface the error in the panel
        err_insight = {"text": f"⚠️ LLM error: {source}", "source": "error", "timestamp": time.time()}
        broadcast_event("ai_insight", err_insight)
        print(f"[Dashboard/LLM] LLM failed: {source}")

# ── OHLCV History ──────────────────────────────────────────────────────────────
HISTORY_DB = os.getenv("HISTORY_DB", "/app/data/dashboard_history.db")
BUCKET_SIZE = 60  # 1-minute candles

PERIOD_SECONDS = {
    "1h": 3600,
    "8h": 28800,
    "1d": 86400,
    "1w": 604800,
    "1m": 2592000,
}


def init_history_db():
    os.makedirs(os.path.dirname(HISTORY_DB), exist_ok=True)
    conn = sqlite3.connect(HISTORY_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ohlcv (
            symbol  TEXT,
            bucket  INTEGER,
            open    REAL,
            high    REAL,
            low     REAL,
            close   REAL,
            volume  INTEGER,
            PRIMARY KEY (symbol, bucket)
        )
    """)
    conn.commit()
    conn.close()


def record_trade(symbol, price, qty, ts):
    if not symbol or price <= 0:
        return
    bucket = int(ts // BUCKET_SIZE) * BUCKET_SIZE
    try:
        conn = sqlite3.connect(HISTORY_DB)
        existing = conn.execute(
            "SELECT open FROM ohlcv WHERE symbol=? AND bucket=?", (symbol, bucket)
        ).fetchone()
        if existing:
            conn.execute(
                "UPDATE ohlcv SET high=MAX(high,?), low=MIN(low,?), close=?, volume=volume+? "
                "WHERE symbol=? AND bucket=?",
                (price, price, price, qty, symbol, bucket),
            )
        else:
            conn.execute(
                "INSERT INTO ohlcv VALUES (?,?,?,?,?,?,?)",
                (symbol, bucket, price, price, price, price, qty),
            )
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[History] Error recording trade: {e}")


def load_securities_file():
    securities = {}
    try:
        with open(Config.SECURITIES_FILE) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                parts = line.split()
                if len(parts) >= 3:
                    securities[parts[0]] = {
                        "start": float(parts[1]),
                        "current": float(parts[2]),
                    }
    except Exception as e:
        print(f"[Dashboard] Cannot read securities: {e}")
    return securities


def save_securities_file(securities):
    with open(Config.SECURITIES_FILE, "w") as f:
        f.write("#SYMBOL\t<start_price>\t<current_price>\n")
        for sym, vals in securities.items():
            f.write(f"{sym}\t{vals['start']:.2f}\t{vals['current']:.2f}\n")


# ── SSE broadcast ──────────────────────────────────────────────────────────────

def broadcast_event(event_type, data):
    message = f"event: {event_type}\ndata: {json.dumps(data)}\n\n"
    with sse_clients_lock:
        dead = []
        for q in sse_clients:
            try:
                q.put_nowait(message)
            except Exception:
                dead.append(q)
        for q in dead:
            sse_clients.remove(q)


# ── Kafka consumer thread ──────────────────────────────────────────────────────

def consume_kafka():
    consumer = create_consumer(
        topics=[Config.ORDERS_TOPIC, Config.SNAPSHOTS_TOPIC, Config.TRADES_TOPIC, Config.AI_INSIGHTS_TOPIC],
        group_id="dashboard",
        component_name="Dashboard",
    )
    for msg in consumer:
        with lock:
            if msg.topic == "orders":
                order = msg.value
                orders.insert(0, order)
                orders[:] = orders[:50]
                broadcast_event("order", order)

            elif msg.topic == "snapshots":
                snap = msg.value
                symbol = snap.get("symbol")
                if not symbol:
                    continue
                bbo_data = {
                    "best_bid": snap.get("best_bid"),
                    "best_ask": snap.get("best_ask"),
                    "bid_size": snap.get("bid_size"),
                    "ask_size": snap.get("ask_size"),
                    "timestamp": snap.get("timestamp", time.time()),
                    "source": snap.get("source", "unknown"),
                }
                bbos[symbol] = bbo_data
                broadcast_event("snapshot", {"symbol": symbol, **bbo_data})

            elif msg.topic == "trades":
                trade = msg.value
                trades_cache.insert(0, trade)
                trades_cache[:] = trades_cache[:200]
                broadcast_event("trade", trade)
                # Record for OHLCV history
                sym = trade.get("symbol", "")
                price = float(trade.get("price") or 0)
                qty = int(trade.get("quantity") or trade.get("qty") or 0)
                ts = float(trade.get("timestamp") or time.time())
                record_trade(sym, price, qty, ts)

            elif msg.topic == Config.AI_INSIGHTS_TOPIC:
                insight = msg.value
                ai_insights_cache.insert(0, insight)
                ai_insights_cache[:] = ai_insights_cache[:10]
                broadcast_event("ai_insight", insight)


# Initialise DB then start consumer thread
init_history_db()
threading.Thread(target=consume_kafka, daemon=True).start()


# ── Flask routes ───────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html", frontend_url=FRONTEND_URL)


@app.route("/health")
def health():
    status = {
        "status": "healthy",
        "service": "dashboard",
        "timestamp": time.time(),
        "stats": {
            "orders_cached": len(orders),
            "trades_cached": len(trades_cache),
            "symbols_tracked": len(bbos),
            "sse_clients": len(sse_clients),
        },
    }
    try:
        r = requests.get(f"{Config.MATCHER_URL}/health", timeout=2)
        status["matcher"] = "connected" if r.status_code == 200 else f"error: {r.status_code}"
    except Exception as e:
        status["matcher"] = f"error: {e}"
        status["status"] = "degraded"
    return jsonify(status)


@app.route("/data")
def data():
    try:
        r = requests.get(f"{Config.MATCHER_URL}/trades", timeout=2)
        resp = r.json()
        trades = resp.get("trades", []) if isinstance(resp, dict) else resp
    except Exception as e:
        print("Cannot fetch trades:", e)
        with lock:
            trades = list(trades_cache)

    try:
        r = requests.get(f"{Config.MATCHER_URL}/orderbook/ALPHA", timeout=2)
        book = r.json()
    except Exception:
        book = {"bids": [], "asks": []}

    with lock:
        return jsonify({"orders": list(orders), "bbos": dict(bbos), "trades": trades, "book": book})


@app.route("/orderbook/<symbol>")
def orderbook(symbol):
    try:
        r = requests.get(f"{Config.MATCHER_URL}/orderbook/{symbol}", timeout=2)
        return (r.text, r.status_code, {"Content-Type": "application/json"})
    except Exception as e:
        return jsonify({"error": str(e), "bids": [], "asks": []}), 500


# Kafka producer (lazy init)
_producer = None


def get_producer():
    global _producer
    if _producer is None:
        _producer = create_producer(component_name="Dashboard")
    return _producer


@app.route("/order/cancel", methods=["POST"])
def cancel_order():
    try:
        data = request.get_json()
        orig_cl_ord_id = data.get("orig_cl_ord_id")
        symbol = data.get("symbol")
        if not orig_cl_ord_id:
            return jsonify({"status": "error", "error": "Missing orig_cl_ord_id"}), 400
        cancel_msg = {
            "type": "cancel",
            "orig_cl_ord_id": orig_cl_ord_id,
            "symbol": symbol,
            "timestamp": time.time(),
        }
        p = get_producer()
        p.send(Config.ORDERS_TOPIC, cancel_msg)
        p.flush()
        return jsonify({"status": "ok", "message": f"Cancel request sent for {orig_cl_ord_id}"})
    except Exception as e:
        return jsonify({"status": "error", "error": str(e)}), 500


@app.route("/order/amend", methods=["POST"])
def amend_order():
    try:
        data = request.get_json()
        orig_cl_ord_id = data.get("orig_cl_ord_id")
        if not orig_cl_ord_id:
            return jsonify({"status": "error", "error": "Missing orig_cl_ord_id"}), 400
        amend_msg = {
            "type": "amend",
            "orig_cl_ord_id": orig_cl_ord_id,
            "cl_ord_id": f"amend-{int(time.time()*1000)}",
            "symbol": data.get("symbol"),
            "quantity": data.get("quantity"),
            "price": data.get("price"),
            "timestamp": time.time(),
        }
        p = get_producer()
        p.send(Config.ORDERS_TOPIC, amend_msg)
        p.flush()
        return jsonify({"status": "ok", "message": f"Amend request sent for {orig_cl_ord_id}"})
    except Exception as e:
        return jsonify({"status": "error", "error": str(e)}), 500


# ── Market schedule ─────────────────────────────────────────────────────────────

def load_market_schedule():
    schedule = {}
    try:
        with open(SCHEDULE_FILE) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                parts = line.split()
                if len(parts) >= 2:
                    schedule[parts[0].lower()] = parts[1]
    except Exception as e:
        print(f"[Scheduler] Cannot read schedule file {SCHEDULE_FILE}: {e}")
    return schedule


def _local_now(tz_name):
    """Return current datetime in the given IANA timezone (e.g. 'Europe/Athens')."""
    try:
        from zoneinfo import ZoneInfo
        return datetime.datetime.now(ZoneInfo(tz_name)).replace(tzinfo=None)
    except Exception:
        pass
    try:
        import pytz
        tz = pytz.timezone(tz_name)
        return datetime.datetime.now(tz).replace(tzinfo=None)
    except Exception:
        pass
    return datetime.datetime.utcnow()


def _do_session_start():
    securities = load_securities_file()
    if securities:
        for sym in securities:
            securities[sym]["current"] = securities[sym]["start"]
        save_securities_file(securities)
    p = get_producer()
    p.send(Config.CONTROL_TOPIC, {"action": "start"})
    p.flush()
    session_state["active"] = True
    session_state["suspended"] = False
    session_state["start_time"] = time.time()
    broadcast_event("session", {"status": "started", "time": session_state["start_time"]})


def _do_session_end():
    securities = load_securities_file()
    for sym in list(securities.keys()):
        try:
            r = requests.get(f"{Config.MATCHER_URL}/orderbook/{sym}", timeout=2)
            book = r.json()
            bids = book.get("bids", [])
            asks = book.get("asks", [])
            if bids and asks:
                best_bid = max(b["price"] for b in bids)
                best_ask = min(a["price"] for a in asks)
                securities[sym]["current"] = round((best_bid + best_ask) / 2, 2)
        except Exception:
            pass
    if securities:
        save_securities_file(securities)
    p = get_producer()
    p.send(Config.CONTROL_TOPIC, {"action": "stop"})
    p.flush()
    session_state["active"] = False
    session_state["suspended"] = False
    broadcast_event("session", {"status": "ended", "time": time.time()})


def schedule_runner():
    """Background thread: auto start/end session based on market_schedule.txt."""
    while True:
        try:
            if session_state.get("mode") == "automatic":
                sched = load_market_schedule()
                start_str = sched.get("start")
                end_str   = sched.get("end")
                tz_name   = sched.get("timezone", "UTC")
                if start_str and end_str:
                    now = _local_now(tz_name)
                    sh, sm = int(start_str.split(":")[0]), int(start_str.split(":")[1])
                    eh, em = int(end_str.split(":")[0]),   int(end_str.split(":")[1])
                    start_t = now.replace(hour=sh, minute=sm, second=0, microsecond=0)
                    end_t   = now.replace(hour=eh, minute=em, second=0, microsecond=0)
                    print(f"[Scheduler] Local time ({tz_name}): {now.strftime('%H:%M')}  window {start_str}-{end_str}")
                    if now >= end_t and session_state["active"]:
                        print("[Scheduler] Auto end of day")
                        _do_session_end()
                    elif start_t <= now < end_t and not session_state["active"]:
                        print("[Scheduler] Auto start of day")
                        _do_session_start()
        except Exception as e:
            print(f"[Scheduler] Error: {e}")
        time.sleep(30)


# ── Session endpoints ──────────────────────────────────────────────────────────

@app.route("/session/start", methods=["POST"])
def session_start():
    try:
        _do_session_start()
        return jsonify({"status": "ok", "message": "Day started"})
    except Exception as e:
        return jsonify({"status": "error", "error": str(e)}), 500


@app.route("/session/end", methods=["POST"])
def session_end():
    try:
        _do_session_end()
        return jsonify({"status": "ok", "message": "Day ended, closing prices saved"})
    except Exception as e:
        return jsonify({"status": "error", "error": str(e)}), 500


@app.route("/session/suspend", methods=["POST"])
def session_suspend():
    try:
        if not session_state["active"]:
            return jsonify({"status": "error", "error": "No active session"}), 400
        p = get_producer()
        p.send(Config.CONTROL_TOPIC, {"action": "suspend"})
        p.flush()
        session_state["suspended"] = True
        broadcast_event("session", {"status": "suspended"})
        return jsonify({"status": "ok", "message": "Session suspended"})
    except Exception as e:
        return jsonify({"status": "error", "error": str(e)}), 500


@app.route("/session/resume", methods=["POST"])
def session_resume():
    try:
        if not session_state["active"]:
            return jsonify({"status": "error", "error": "No active session"}), 400
        p = get_producer()
        p.send(Config.CONTROL_TOPIC, {"action": "resume"})
        p.flush()
        session_state["suspended"] = False
        broadcast_event("session", {"status": "active"})
        return jsonify({"status": "ok", "message": "Session resumed"})
    except Exception as e:
        return jsonify({"status": "error", "error": str(e)}), 500


@app.route("/session/ai_insight", methods=["POST"])
def trigger_ai_insight():
    threading.Thread(target=_generate_and_broadcast, daemon=True).start()
    return jsonify({"status": "ok", "message": "Insight generation started"})


@app.route("/ai/debug")
def ai_debug():
    """Synchronous LLM test — returns raw API result for debugging."""
    result = {
        "hf_token_set": bool(HF_TOKEN),
        "hf_token_prefix": HF_TOKEN[:8] + "…" if HF_TOKEN else None,
        "hf_model": HF_MODEL,
        "hf_url": HF_URL,
        "ollama_host": OLLAMA_HOST,
    }
    if not HF_TOKEN:
        result["error"] = "HF_TOKEN not set"
        return jsonify(result)
    try:
        r = requests.post(
            HF_URL,
            headers={"Authorization": f"Bearer {HF_TOKEN}",
                     "Content-Type": "application/json"},
            json={"model": HF_MODEL,
                  "messages": [{"role": "user", "content": "Reply with exactly: OK"}],
                  "max_tokens": 10},
            timeout=30,
        )
        result["http_status"] = r.status_code
        result["response_body"] = r.text[:500]
        try:
            result["response_json"] = r.json()
        except Exception:
            pass
    except Exception as e:
        result["exception"] = str(e)
    return jsonify(result)


@app.route("/session/mode", methods=["POST"])
def session_mode():
    try:
        current = session_state.get("mode", "manual")
        new_mode = "automatic" if current == "manual" else "manual"
        session_state["mode"] = new_mode
        broadcast_event("mode", {"mode": new_mode})
        return jsonify({"status": "ok", "mode": new_mode})
    except Exception as e:
        return jsonify({"status": "error", "error": str(e)}), 500


@app.route("/session/status")
def session_status():
    return jsonify(session_state)


# ── History endpoint ───────────────────────────────────────────────────────────

@app.route("/history/<symbol>")
def history(symbol):
    period = request.args.get("period", "1d")
    seconds = PERIOD_SECONDS.get(period, 86400)
    since = int(time.time()) - seconds
    try:
        conn = sqlite3.connect(HISTORY_DB)
        rows = conn.execute(
            "SELECT bucket, open, high, low, close, volume FROM ohlcv "
            "WHERE symbol=? AND bucket>=? ORDER BY bucket ASC",
            (symbol, since),
        ).fetchall()
        conn.close()
    except Exception as e:
        return jsonify({"error": str(e), "candles": []}), 500

    candles = [{"t": r[0], "o": r[1], "h": r[2], "l": r[3], "c": r[4], "v": r[5]} for r in rows]

    # Aggregate to at most 150 bars for display
    if len(candles) > 150:
        step = len(candles) // 150 + 1
        agg = []
        for i in range(0, len(candles), step):
            chunk = candles[i : i + step]
            agg.append({
                "t": chunk[0]["t"],
                "o": chunk[0]["o"],
                "h": max(c["h"] for c in chunk),
                "l": min(c["l"] for c in chunk),
                "c": chunk[-1]["c"],
                "v": sum(c["v"] for c in chunk),
            })
        candles = agg

    return jsonify({"symbol": symbol, "period": period, "candles": candles})


# ── SSE stream ─────────────────────────────────────────────────────────────────

@app.route("/stream")
def stream():
    def event_stream():
        q = Queue(maxsize=100)
        with sse_clients_lock:
            sse_clients.append(q)
        try:
            yield f"event: connected\ndata: {json.dumps({'status': 'connected'})}\n\n"
            with lock:
                yield (
                    f"event: init\ndata: "
                    f"{json.dumps({'orders': list(orders), 'bbos': dict(bbos), 'trades': list(trades_cache)})}\n\n"
                )
                yield f"event: ai_insights_init\ndata: {json.dumps(list(ai_insights_cache))}\n\n"
            # Also send current session state
            if not session_state["active"]:
                _sess_status = "ended"
            elif session_state["suspended"]:
                _sess_status = "suspended"
            else:
                _sess_status = "started"
            yield f"event: session\ndata: {json.dumps({'status': _sess_status})}\n\n"
            yield f"event: mode\ndata: {json.dumps({'mode': session_state.get('mode', 'manual')})}\n\n"
            while True:
                try:
                    message = q.get(timeout=30)
                    yield message
                except Empty:
                    yield ": keepalive\n\n"
        finally:
            with sse_clients_lock:
                if q in sse_clients:
                    sse_clients.remove(q)

    return Response(
        event_stream(),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


_scheduler = threading.Thread(target=schedule_runner, daemon=True)
_scheduler.start()

if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=True, use_reloader=False)
