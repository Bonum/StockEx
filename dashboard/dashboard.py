import sys
sys.path.insert(0, "/app")

from flask import Flask, render_template, jsonify, Response, request
import threading, json, os, time, requests
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

def broadcast_event(event_type, data):
    """Send an event to all connected SSE clients."""
    message = f"event: {event_type}\ndata: {json.dumps(data)}\n\n"
    with sse_clients_lock:
        dead_clients = []
        for q in sse_clients:
            try:
                q.put_nowait(message)
            except:
                dead_clients.append(q)
        for q in dead_clients:
            sse_clients.remove(q)

# Kafka consumer thread
def consume_kafka():
    consumer = create_consumer(
        topics=[Config.ORDERS_TOPIC, Config.SNAPSHOTS_TOPIC, Config.TRADES_TOPIC],
        group_id="dashboard",
        component_name="Dashboard"
    )
    for msg in consumer:
        with lock:
            if msg.topic == "orders":
                order = msg.value
                orders.insert(0, order)
                orders[:] = orders[:50]  # keep last 50
                broadcast_event("order", order)

            elif msg.topic == "snapshots":
                snap = msg.value
                # Handle both simple snapshots and MDF snapshots
                symbol = snap.get("symbol")
                if not symbol:
                    continue

                bbo_data = {
                    "best_bid": snap.get("best_bid"),
                    "best_ask": snap.get("best_ask"),
                    "bid_size": snap.get("bid_size"),
                    "ask_size": snap.get("ask_size"),
                    "timestamp": snap.get("timestamp", time.time()),
                    "source": snap.get("source", "unknown")
                }
                bbos[symbol] = bbo_data
                broadcast_event("snapshot", {"symbol": symbol, **bbo_data})

            elif msg.topic == "trades":
                trade = msg.value
                trades_cache.insert(0, trade)
                trades_cache[:] = trades_cache[:200]  # keep last 200
                broadcast_event("trade", trade)

threading.Thread(target=consume_kafka, daemon=True).start()

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/health")
def health():
    """Health check endpoint."""
    status = {
        "status": "healthy",
        "service": "dashboard",
        "timestamp": time.time(),
        "stats": {
            "orders_cached": len(orders),
            "trades_cached": len(trades_cache),
            "symbols_tracked": len(bbos),
            "sse_clients": len(sse_clients)
        }
    }
    # Check matcher connectivity
    try:
        r = requests.get(f"{Config.MATCHER_URL}/health", timeout=2)
        if r.status_code == 200:
            status["matcher"] = "connected"
        else:
            status["matcher"] = f"error: status {r.status_code}"
            status["status"] = "degraded"
    except Exception as e:
        status["matcher"] = f"error: {e}"
        status["status"] = "degraded"

    return jsonify(status)

@app.route("/data")
def data():
    """Fallback polling endpoint (still useful for initial load or SSE fallback)."""
    # Always fetch trades from matcher (more reliable)
    try:
        r = requests.get(f"{Config.MATCHER_URL}/trades", timeout=2)
        resp = r.json()
        trades = resp.get('trades', []) if isinstance(resp, dict) else resp
    except Exception as e:
        print("Cannot fetch trades:", e)
        with lock:
            trades = list(trades_cache)

    # Order book for dropdown (fetch latest for all symbols)
    try:
        r = requests.get(f"{Config.MATCHER_URL}/orderbook/ALPHA", timeout=2)
        book = r.json()
    except Exception as e:
        print("Cannot fetch orderbook:", e)
        book = {"bids": [], "asks": []}

    with lock:
        return jsonify({
            "orders": list(orders),
            "bbos": dict(bbos),
            "trades": trades,
            "book": book
        })

@app.route("/orderbook/<symbol>")
def orderbook(symbol):
    """Proxy to matcher orderbook API."""
    try:
        r = requests.get(f"{Config.MATCHER_URL}/orderbook/{symbol}", timeout=2)
        return (r.text, r.status_code, {"Content-Type": "application/json"})
    except Exception as e:
        return jsonify({"error": str(e), "bids": [], "asks": []}), 500

# Kafka producer for order management (lazy init)
_producer = None
def get_producer():
    global _producer
    if _producer is None:
        _producer = create_producer(component_name="Dashboard")
    return _producer

@app.route("/order/cancel", methods=["POST"])
def cancel_order():
    """Send cancel request to matcher via Kafka."""
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
            "timestamp": time.time()
        }

        producer = get_producer()
        producer.send(Config.ORDERS_TOPIC, cancel_msg)
        producer.flush()

        return jsonify({"status": "ok", "message": f"Cancel request sent for {orig_cl_ord_id}"})
    except Exception as e:
        return jsonify({"status": "error", "error": str(e)}), 500

@app.route("/order/amend", methods=["POST"])
def amend_order():
    """Send amend request to matcher via Kafka."""
    try:
        data = request.get_json()
        orig_cl_ord_id = data.get("orig_cl_ord_id")
        symbol = data.get("symbol")
        quantity = data.get("quantity")
        price = data.get("price")

        if not orig_cl_ord_id:
            return jsonify({"status": "error", "error": "Missing orig_cl_ord_id"}), 400

        amend_msg = {
            "type": "amend",
            "orig_cl_ord_id": orig_cl_ord_id,
            "cl_ord_id": f"amend-{int(time.time()*1000)}",
            "symbol": symbol,
            "quantity": quantity,
            "price": price,
            "timestamp": time.time()
        }

        producer = get_producer()
        producer.send(Config.ORDERS_TOPIC, amend_msg)
        producer.flush()

        return jsonify({"status": "ok", "message": f"Amend request sent for {orig_cl_ord_id}"})
    except Exception as e:
        return jsonify({"status": "error", "error": str(e)}), 500

@app.route("/stream")
def stream():
    """SSE endpoint for real-time updates."""
    def event_stream():
        q = Queue(maxsize=100)
        with sse_clients_lock:
            sse_clients.append(q)
        try:
            # Send initial connection event
            yield f"event: connected\ndata: {json.dumps({'status': 'connected'})}\n\n"

            # Send current state on connect (including trades)
            with lock:
                yield f"event: init\ndata: {json.dumps({'orders': list(orders), 'bbos': dict(bbos), 'trades': list(trades_cache)})}\n\n"

            while True:
                try:
                    message = q.get(timeout=30)  # 30s timeout for keepalive
                    yield message
                except Empty:
                    # Send keepalive comment
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
            "X-Accel-Buffering": "no",  # Disable nginx buffering
            "Connection": "keep-alive"
        }
    )

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True, use_reloader=False)
