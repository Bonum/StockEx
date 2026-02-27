#!/usr/bin/env python3
import sys
sys.path.insert(0, "/app")

import quickfix as fix
import quickfix44 as fix44
from flask import Flask, render_template, request, jsonify
import json
import threading, time, os
from collections import deque
from threading import Lock

from shared.config import Config
from shared.kafka_utils import create_producer

app = Flask(__name__)

fix_initiator = None
fix_app = None
_messages = deque(maxlen=500)
_msgs_lock = Lock()

# --- Global OrderID counter ---
LOCK = threading.Lock()

def next_order_id():
    os.makedirs(os.path.dirname(Config.ORDER_ID_FILE), exist_ok=True)

    with LOCK:  # thread safety inside container
        if not os.path.exists(Config.ORDER_ID_FILE):
            with open(Config.ORDER_ID_FILE, "w") as f:
                f.write("0")

        with open(Config.ORDER_ID_FILE, "r+") as f:
            current = int(f.read().strip() or 0)
            new_id = current + 1
            f.seek(0)
            f.write(str(new_id))
            f.truncate()

    return new_id


def log(msg: str):
    with _msgs_lock:
        _messages.append(msg)

class FixUIClientApp(fix.Application):
    def __init__(self):
        super().__init__()
        self.sessionID = None
        self.connected = False
        self.producer = create_producer(component_name="FIX-UI-Client")

    def onCreate(self, sessionID): self.sessionID = sessionID
    def onLogon(self, sessionID):
        self.connected = True
        self.sessionID = sessionID
        log(f"✅ Logon: {sessionID}")
    def onLogout(self, sessionID):
        self.connected = False
        log(f"❌ Logout: {sessionID}")

    def toAdmin(self, message, sessionID): pass
    def fromAdmin(self, message, sessionID): pass

    def toApp(self, message, sessionID):
        log(f"➡️ Sent App: {message.toString()}")

    def fromApp(self, message, sessionID):
        msg_type = fix.MsgType()
        message.getHeader().getField(msg_type)
        if msg_type.getValue() == fix.MsgType_ExecutionReport:
            execType, ordStatus, clOrdID = fix.ExecType(), fix.OrdStatus(), fix.ClOrdID()
            try: message.getField(execType)
            except: pass
            try: message.getField(ordStatus)
            except: pass
            try: message.getField(clOrdID)
            except: pass

            status_map = {
                fix.ExecType_NEW: "New",
                fix.ExecType_PARTIAL_FILL: "Partial Fill",
                fix.ExecType_FILL: "Fill",
                fix.ExecType_CANCELED: "Canceled",
                fix.ExecType_REPLACED: "Replaced",
                fix.ExecType_REJECTED: "Rejected",
            }
            status = status_map.get(execType.getValue(), f"ExecType={execType.getValue()}")
            log(f"📥 ExecReport: ClOrdID={clOrdID.getValue() if clOrdID else '?'} Status={status}")
        else:
            log(f"📩 App: {message.toString()}")

    def send_order(self, side, symbol, qty, price):
        if not self.sessionID:
            return "⚠️ No FIX session active"

        order = fix44.NewOrderSingle()
        cl_ord_id = next_order_id()
        order.setField(fix.ClOrdID(str(cl_ord_id)))
        order.setField(fix.HandlInst('1'))
        order.setField(fix.Symbol(symbol))
        order.setField(fix.Side(side))
        order.setField(fix.TransactTime())
        order.setField(fix.OrdType(fix.OrdType_LIMIT))
        order.setField(fix.OrderQty(qty))
        order.setField(fix.Price(price))

        fix.Session.sendToTarget(order, self.sessionID)
        log(f"📤 Sent Order (ID={cl_ord_id}): {order.toString()}")
        
        # 🔥 Publish to Kafka
        self.producer.send(Config.ORDERS_TOPIC,
        {
            "id": cl_ord_id,
            "symbol": symbol,
            "side": "buy" if side == "1" else "sell",
            "qty": qty,
            "price": price,
            "timestamp": time.time(),
        })

        return "Order sent!"
     
# --- FIX lifecycle ---
def start_fix():
    global fix_initiator, fix_app
    if fix_initiator is not None:
        log("ℹ️ FIX already starting/started")
        return
    settings = fix.SessionSettings(CONFIG_FILE)
    fix_app = FixUIClientApp()
    store = fix.FileStoreFactory(settings)
    logfile = fix.FileLogFactory(settings)
    fix_initiator = fix.SocketInitiator(fix_app, store, settings, logfile)
    fix_initiator.start()
    log(f"🔌 FIX initiator started with {CONFIG_FILE}")

def stop_fix():
    global fix_initiator
    if fix_initiator:
        fix_initiator.stop()
        log("🪫 FIX initiator stopped")
        fix_initiator = None

def load_securities():
    securities = {}
    try:
        with open(Config.SECURITIES_FILE) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    parts = line.split()
                    if len(parts) >= 3:
                        securities[parts[0]] = float(parts[2])
                    elif len(parts) >= 1:
                        securities[parts[0]] = 10.0
    except Exception:
        pass
    return securities or {"ALPHA": 5.65, "PEIR": 8.35, "EXAE": 6.90, "QUEST": 13.35, "NBG": 8.00}

# --- Flask routes ---
@app.route("/")
def index():
    with _msgs_lock:
        msgs = list(reversed(_messages))   # newest first
    connected = bool(fix_app and fix_app.connected)
    return render_template("index.html", messages=msgs, connected=connected,
                           securities=load_securities())

@app.route("/status")
def status():
    return jsonify({"connected": bool(fix_app and fix_app.connected)})

@app.route("/connect")
def connect():
    threading.Thread(target=start_fix, daemon=True).start()
    return jsonify({"status": "ok", "message": "Connecting..."})

@app.route("/disconnect")
def disconnect():
    stop_fix()
    return jsonify({"status": "ok", "message": "Disconnected"})

@app.route("/order", methods=["POST"])
def order():
    if not fix_app or not fix_app.connected:
        log("⚠️ Tried to send while disconnected")
        return jsonify({"status": "error", "message": "Not connected"}), 400
    data = request.get_json(force=True) or {}
    side     = data.get("side", "buy")
    side_tag = "1" if side.lower() == "buy" else "2"
    symbol   = data.get("symbol", "FOO")
    qty      = float(data.get("qty", 100))
    price    = float(data.get("price", 10))
    fix_app.send_order(side_tag, symbol, qty, price)
    return jsonify({"status": "ok", "message": "Order sent"})

@app.route("/messages")
def messages_route():
    with _msgs_lock:
        msgs = list(reversed(_messages))
    return jsonify(msgs)

# --- Configurable ---
CONFIG_FILE = os.getenv("FIX_CONFIG", "client.cfg")
PORT = int(os.getenv("UI_PORT", "5002"))

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)
