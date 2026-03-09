"""Clearing House portal — Flask service on port 5004.

Routes are mounted at /ch/ so nginx can proxy /ch/ → this service
without stripping the prefix (proxy_pass http://...:5004).
"""

import sys
sys.path.insert(0, "/app")

import json
import os
import threading
import time
import datetime
from queue import Empty, Queue

import requests
from flask import (
    Flask, Response, jsonify, redirect, render_template,
    request, session, stream_with_context, url_for,
)

from shared.config import Config
from shared.kafka_utils import create_producer

import ch_database as db
import ch_ai_trader as ai_trader

# ── App setup ──────────────────────────────────────────────────────────────────
app = Flask(__name__, template_folder="templates")
app.secret_key = os.getenv("CH_SECRET_KEY", "ch-stockex-secret-2024")
app.config["TEMPLATES_AUTO_RELOAD"] = True


@app.template_filter("ts")
def ts_filter(ts):
    """Format a Unix timestamp as HH:MM:SS."""
    return datetime.datetime.fromtimestamp(float(ts)).strftime("%H:%M:%S")

MATCHER_URL = os.getenv("MATCHER_URL", Config.MATCHER_URL)
SECURITIES_FILE = os.getenv("SECURITIES_FILE", "/app/shared/data/securities.txt")

# SSE clients
_sse_clients: list[Queue] = []
_sse_lock = threading.Lock()

_producer = None


def get_producer():
    global _producer
    if _producer is None:
        _producer = create_producer(component_name="CH-App")
    return _producer


# ── Helpers ────────────────────────────────────────────────────────────────────

def _load_symbols() -> list[str]:
    symbols = []
    try:
        with open(SECURITIES_FILE) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    parts = line.split()
                    if parts:
                        symbols.append(parts[0])
    except Exception:
        pass
    return symbols


def _get_bbos() -> dict:
    symbols = _load_symbols()
    bbos = {}
    for sym in symbols:
        try:
            r = requests.get(f"{MATCHER_URL}/orderbook/{sym}", timeout=2)
            if r.status_code == 200:
                book = r.json()
                bids = book.get("bids", [])
                asks = book.get("asks", [])
                best_bid = max((b["price"] for b in bids), default=None)
                best_ask = min((a["price"] for a in asks), default=None)
                mid = round((best_bid + best_ask) / 2, 2) if best_bid and best_ask else None
                bbos[sym] = {"best_bid": best_bid, "best_ask": best_ask, "mid": mid}
        except Exception:
            pass
    return bbos


def _broadcast(event_type: str, data: dict):
    msg = f"event: {event_type}\ndata: {json.dumps(data)}\n\n"
    with _sse_lock:
        for q in _sse_clients:
            try:
                q.put_nowait(msg)
            except Exception:
                pass


def _build_leaderboard(bbos: dict) -> list[dict]:
    rows = db.get_leaderboard()
    for row in rows:
        holdings_value = sum(
            h["quantity"] * (bbos.get(h["symbol"], {}).get("mid") or h["avg_cost"])
            for h in row["holdings"]
        )
        row["holdings_value"] = round(holdings_value, 2)
        row["total_value"] = round(row["capital"] + holdings_value, 2)
        row["pnl"] = round(row["total_value"] - db.CH_STARTING_CAPITAL, 2)
        row["is_human"] = ai_trader.is_human_active(row["member_id"])
        row["ai_type"] = _member_ai_type(row["member_id"])
    # Sort by total_value descending
    rows.sort(key=lambda r: r["total_value"], reverse=True)
    for i, row in enumerate(rows):
        row["rank"] = i + 1
    return rows


# ── Auth helpers ───────────────────────────────────────────────────────────────

def current_member() -> str | None:
    return session.get("member_id")


def require_login():
    if not current_member():
        return redirect(url_for("login_page"))
    return None


# ── Routes ─────────────────────────────────────────────────────────────────────

@app.route("/ch/")
def index():
    bbos = _get_bbos()
    leaderboard = _build_leaderboard(bbos)
    member = current_member()
    return render_template(
        "dashboard.html",
        leaderboard=leaderboard,
        symbols=list(bbos.keys()),
        member=member,
        obligation=db.CH_DAILY_OBLIGATION,
    )


@app.route("/ch/login", methods=["GET"])
def login_page():
    if current_member():
        return redirect(url_for("portfolio"))
    return render_template("login.html", members=db.CH_MEMBERS)


@app.route("/ch/login", methods=["POST"])
def login_post():
    member_id = request.form.get("member_id", "").upper().strip()
    password  = request.form.get("password", "").strip()

    if not db.verify_password(member_id, password):
        return render_template("login.html", members=db.CH_MEMBERS, error="Invalid credentials.")

    session["member_id"] = member_id
    ai_trader.set_human_active(member_id)
    _broadcast("member_login", {"member_id": member_id})
    return redirect(url_for("portfolio"))


@app.route("/ch/logout", methods=["POST"])
def logout():
    mid = current_member()
    if mid:
        ai_trader.set_human_inactive(mid)
        session.pop("member_id", None)
        _broadcast("member_logout", {"member_id": mid})
    return redirect(url_for("index"))


@app.route("/ch/portfolio")
def portfolio():
    redir = require_login()
    if redir:
        return redir
    mid = current_member()
    member = db.get_member(mid)
    holdings = db.get_holdings(mid)
    daily = db.get_daily_trades(mid)
    trades = db.get_trade_log(mid)
    settlements = db.get_settlements(mid, limit=10)
    bbos = _get_bbos()

    # Enrich holdings with current prices and P&L
    for h in holdings:
        bbo = bbos.get(h["symbol"], {})
        current_price = bbo.get("mid") or h["avg_cost"]
        h["current_price"] = round(current_price, 2)
        h["unrealized_pnl"] = round((current_price - h["avg_cost"]) * h["quantity"], 2)
        h["value"] = round(current_price * h["quantity"], 2)

    total_holdings_value = sum(h["value"] for h in holdings)
    total_value = round(member["capital"] + total_holdings_value, 2)
    total_pnl   = round(total_value - db.CH_STARTING_CAPITAL, 2)

    return render_template(
        "portfolio.html",
        member=mid,
        capital=round(member["capital"], 2),
        holdings=holdings,
        daily=daily,
        trades=trades,
        settlements=settlements,
        symbols=list(bbos.keys()),
        obligation=db.CH_DAILY_OBLIGATION,
        total_value=total_value,
        total_pnl=total_pnl,
    )


@app.route("/ch/order", methods=["POST"])
def submit_order():
    redir = require_login()
    if redir:
        return jsonify({"error": "Not logged in"}), 401

    mid = current_member()
    data = request.get_json(force=True)

    symbol   = str(data.get("symbol", "")).upper()
    side     = str(data.get("side", "")).upper()
    quantity = int(data.get("quantity", 0))
    price    = float(data.get("price", 0))

    if side not in ("BUY", "SELL") or quantity <= 0 or price <= 0 or not symbol:
        return jsonify({"error": "Invalid order parameters"}), 400

    member = db.get_member(mid)
    if not member:
        return jsonify({"error": "Member not found"}), 404

    # Capital check
    if side == "BUY" and quantity * price > member["capital"]:
        return jsonify({"error": f"Insufficient capital (have €{member['capital']:.2f})"}), 400

    # Holdings check
    if side == "SELL":
        holding = db.get_holding(mid, symbol)
        if holding["quantity"] < quantity:
            return jsonify({"error": f"Insufficient holdings ({holding['quantity']} shares held)"}), 400

    cl_ord_id = f"{mid}-{int(time.time()*1000)}-H"
    msg = {
        "cl_ord_id":    cl_ord_id,
        "symbol":       symbol,
        "side":         side,
        "quantity":     quantity,
        "price":        price,
        "ord_type":     "LIMIT",
        "time_in_force": "DAY",
        "timestamp":    time.time(),
        "source":       "CLRH",
    }
    get_producer().send(Config.ORDERS_TOPIC, msg)
    return jsonify({"status": "ok", "cl_ord_id": cl_ord_id})


@app.route("/ch/eod", methods=["POST"])
def eod_settlement():
    """Called by dashboard at end-of-day to settle all CH members."""
    bbos = _get_bbos()
    today = db.today_str()
    results = []

    members = db.get_all_members()
    for member in members:
        mid = member["member_id"]
        capital = member["capital"]
        holdings = db.get_holdings(mid)
        daily = db.get_daily_trades(mid, today)

        # Unrealized P&L: current market value vs avg cost
        unrealized_pnl = 0.0
        for h in holdings:
            bbo = bbos.get(h["symbol"], {})
            current_price = bbo.get("mid") or h["avg_cost"]
            unrealized_pnl += (current_price - h["avg_cost"]) * h["quantity"]

        # Realized P&L approximation: capital change from starting capital
        # minus current holdings cost basis
        cost_basis = sum(h["quantity"] * h["avg_cost"] for h in holdings)
        realized_pnl = round(capital - db.CH_STARTING_CAPITAL + cost_basis, 2)

        obligation_met = daily["total_securities"] >= db.CH_DAILY_OBLIGATION

        db.record_settlement(
            member_id=mid,
            trading_date=today,
            opening_capital=db.CH_STARTING_CAPITAL,
            closing_capital=round(capital, 2),
            realized_pnl=round(realized_pnl, 2),
            unrealized_pnl=round(unrealized_pnl, 2),
            obligation_met=obligation_met,
        )

        results.append({
            "member_id": mid,
            "capital": round(capital, 2),
            "unrealized_pnl": round(unrealized_pnl, 2),
            "obligation_met": obligation_met,
        })

        if not obligation_met:
            print(f"[CH-EOD] {mid} did NOT meet daily obligation "
                  f"({daily['total_securities']}/{db.CH_DAILY_OBLIGATION})")

    _broadcast("eod_settlement", {"date": today, "count": len(results)})
    print(f"[CH-EOD] Settlement complete for {len(results)} members on {today}")
    return jsonify({"status": "ok", "settlements": results})


# ── JSON API ───────────────────────────────────────────────────────────────────

@app.route("/ch/api/leaderboard")
def api_leaderboard():
    bbos = _get_bbos()
    return jsonify(_build_leaderboard(bbos))


@app.route("/ch/api/portfolio")
def api_portfolio():
    mid = current_member()
    if not mid:
        return jsonify({"error": "Not logged in"}), 401
    bbos = _get_bbos()
    member = db.get_member(mid)
    holdings = db.get_holdings(mid)
    daily = db.get_daily_trades(mid)
    for h in holdings:
        bbo = bbos.get(h["symbol"], {})
        h["current_price"] = bbo.get("mid") or h["avg_cost"]
        h["unrealized_pnl"] = round((h["current_price"] - h["avg_cost"]) * h["quantity"], 2)
    return jsonify({
        "member_id": mid,
        "capital": round(member["capital"], 2),
        "holdings": holdings,
        "daily": daily,
    })


@app.route("/ch/api/member/<member_id>")
def api_member_detail(member_id):
    """Full detail for a single member: capital, holdings, last 20 trades."""
    member_id = member_id.upper().strip()
    member = db.get_member(member_id)
    if not member:
        return jsonify({"error": "Member not found"}), 404

    bbos = _get_bbos()
    holdings = db.get_holdings(member_id)
    daily = db.get_daily_trades(member_id)
    trades = db.get_trade_log(member_id, limit=20)

    for h in holdings:
        bbo = bbos.get(h["symbol"], {})
        current_price = bbo.get("mid") or h["avg_cost"]
        h["current_price"] = round(current_price, 2)
        h["value"] = round(current_price * h["quantity"], 2)
        h["unrealized_pnl"] = round((current_price - h["avg_cost"]) * h["quantity"], 2)

    total_holdings_value = sum(h["value"] for h in holdings)
    total_value = round(member["capital"] + total_holdings_value, 2)
    total_pnl = round(total_value - db.CH_STARTING_CAPITAL, 2)

    ai_decisions = db.get_ai_decisions(member_id, limit=10)
    # Parse the stored JSON strings for the frontend
    import json as _json
    for d in ai_decisions:
        if d.get("parsed_order") and isinstance(d["parsed_order"], str):
            try:
                d["parsed_order"] = _json.loads(d["parsed_order"])
            except Exception:
                pass

    return jsonify({
        "member_id": member_id,
        "capital": round(member["capital"], 2),
        "holdings": holdings,
        "holdings_value": round(total_holdings_value, 2),
        "total_value": total_value,
        "pnl": total_pnl,
        "daily": daily,
        "trades": trades,
        "ai_decisions": ai_decisions,
        "is_human": ai_trader.is_human_active(member_id),
        "ai_type": _member_ai_type(member_id),
        "obligation": db.CH_DAILY_OBLIGATION,
    })


def _member_ai_type(member_id: str) -> str:
    if ai_trader.is_human_active(member_id):
        return "Human"
    return ai_trader.get_member_strategy(member_id).upper()


@app.route("/ch/api/market")
def api_market():
    return jsonify(_get_bbos())


@app.route("/ch/api/config")
def api_config():
    result = {
        "member_strategies": ai_trader.get_all_member_strategies(),
        "obligation": db.CH_DAILY_OBLIGATION,
        "ai_interval": int(os.getenv("CH_AI_INTERVAL", "45")),
    }
    try:
        from ch_rl_trader import get_model_info
        result["nn_models"] = get_model_info()
    except Exception:
        pass
    return jsonify(result)


@app.route("/ch/api/member/<member_id>/strategy", methods=["POST"])
def api_set_member_strategy(member_id):
    """Set the AI model type for a specific member."""
    member_id = member_id.upper().strip()
    data = request.get_json(force=True)
    strategy = data.get("strategy", "")
    result = ai_trader.set_member_strategy(member_id, strategy)
    _broadcast("config", {"member_id": member_id, "strategy": result})
    return jsonify({"status": "ok", "member_id": member_id, "strategy": result})


# ── SSE ────────────────────────────────────────────────────────────────────────

@app.route("/ch/stream")
def sse_stream():
    q: Queue = Queue(maxsize=50)
    with _sse_lock:
        _sse_clients.append(q)

    def generate():
        try:
            yield "data: {\"type\":\"connected\"}\n\n"
            while True:
                try:
                    msg = q.get(timeout=30)
                    yield msg
                except Empty:
                    yield ": heartbeat\n\n"
        finally:
            with _sse_lock:
                try:
                    _sse_clients.remove(q)
                except ValueError:
                    pass

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# ── Startup ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    db.init_db()
    ai_trader.start()
    port = int(os.getenv("CH_PORT", "5004"))
    print(f"[CH] Clearing House service starting on port {port}")
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)
