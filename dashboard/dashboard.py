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
lock = threading.Lock()

# SSE: list of queues for connected clients
sse_clients = []
sse_clients_lock = threading.Lock()

# Session state
session_state = {"active": False, "start_time": None, "suspended": False, "mode": "automatic"}

SCHEDULE_FILE = os.getenv("SCHEDULE_FILE", "/app/shared_data/market_schedule.txt")

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
        topics=[Config.ORDERS_TOPIC, Config.SNAPSHOTS_TOPIC, Config.TRADES_TOPIC],
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


# Initialise DB then start consumer thread
init_history_db()
threading.Thread(target=consume_kafka, daemon=True).start()


# ── Flask routes ───────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


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
                if len(parts) == 2:
                    schedule[parts[0].lower()] = parts[1]
    except Exception:
        pass
    return schedule


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
                end_str = sched.get("end")
                if start_str and end_str:
                    now = datetime.datetime.now()
                    sh, sm = int(start_str.split(":")[0]), int(start_str.split(":")[1])
                    eh, em = int(end_str.split(":")[0]), int(end_str.split(":")[1])
                    start_t = now.replace(hour=sh, minute=sm, second=0, microsecond=0)
                    end_t   = now.replace(hour=eh, minute=em, second=0, microsecond=0)
                    if now >= end_t and session_state["active"]:
                        print("[Scheduler] Auto end of day")
                        _do_session_end()
                    elif now >= start_t and not session_state["active"]:
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
