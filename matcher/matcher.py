#!/usr/bin/env python3
"""Simple matcher service:
 - consumes orders from Kafka topic 'orders'
 - maintains in-memory order book per symbol (with SQLite persistence)
 - matches incoming orders using simple price-time priority
 - publishes trades to Kafka 'trades' topic and persists to SQLite
 - exposes HTTP API:
    GET /trades -> recent trades list (from DB with pagination)
    GET /orderbook/<symbol> -> current book for symbol
"""
import sys
sys.path.insert(0, "/app")

import json, time, threading
from collections import defaultdict, deque
from flask import Flask, jsonify, request

from shared.config import Config
from shared.kafka_utils import create_producer, create_consumer
from database import init_db, save_trade, get_trades, get_trade_count, save_order, update_order_quantity, load_order_books

# Initialize database on startup
init_db()

# Load persisted order books from database
_loaded_books = load_order_books()
order_books = defaultdict(lambda: {"bids": [], "asks": []})
order_books.update(_loaded_books)
print(f"Loaded {len(_loaded_books)} symbols from database")

# In-memory trades cache for fast access (DB is source of truth)
trades_log = deque(maxlen=200)
lock = threading.Lock()

app = Flask(__name__)


def normalize_order(raw):
    # Expecting JSON with keys like symbol, price, quantity, side (BUY/SELL) or FIX-like tags
    if not isinstance(raw, dict):
        return None
    payload = raw.get('payload') or raw.get('message') or raw
    def pick(*keys):
        for k in keys:
            if k in payload:
                return payload.get(k)
            if k in raw:
                return raw.get(k)
        return None
    sym = pick('55','Symbol','symbol')
    side = pick('54','Side','side','type')
    qty = pick('38','OrderQty','quantity','qty')
    px = pick('44','Price','price','px')
    cl = pick('11','ClOrdID','cl_ord_id','id')
    ord_type = pick('40','OrdType','ord_type','order_type')  # 1=Market, 2=Limit
    time_in_force = pick('59','TimeInForce','time_in_force','tif')  # 0=Day, 1=GTC, 3=IOC, 4=FOK

    try:
        quantity = int(qty) if qty is not None else 0
    except:
        try:
            quantity = int(float(qty))
        except:
            quantity = 0
    try:
        price = float(px) if px is not None else None
    except:
        price = None

    side_norm = None
    if side is not None:
        s = str(side).lower()
        if s in ('1','buy','b'):
            side_norm = 'BUY'
        elif s in ('2','sell','s'):
            side_norm = 'SELL'
        else:
            # if type field contains 'buy' or 'sell'
            if 'buy' in s:
                side_norm = 'BUY'
            elif 'sell' in s:
                side_norm = 'SELL'
            else:
                side_norm = s.upper()

    # Normalize order type: 1 or 'market' -> MARKET, 2 or 'limit' -> LIMIT
    ord_type_norm = 'LIMIT'  # Default to limit
    if ord_type is not None:
        ot = str(ord_type).lower()
        if ot in ('1', 'market', 'm'):
            ord_type_norm = 'MARKET'
        elif ot in ('2', 'limit', 'l'):
            ord_type_norm = 'LIMIT'

    # Normalize TimeInForce: 0=Day (default), 1=GTC, 3=IOC, 4=FOK
    tif_norm = 'DAY'  # Default
    if time_in_force is not None:
        tif = str(time_in_force).lower()
        if tif in ('0', 'day'):
            tif_norm = 'DAY'
        elif tif in ('1', 'gtc'):
            tif_norm = 'GTC'
        elif tif in ('3', 'ioc'):
            tif_norm = 'IOC'
        elif tif in ('4', 'fok'):
            tif_norm = 'FOK'

    order = {
        'cl_ord_id': cl,
        'symbol': sym,
        'side': side_norm,
        'quantity': quantity,
        'price': price,
        'ord_type': ord_type_norm,
        'time_in_force': tif_norm,
        'raw': raw,
        'timestamp': time.time()
    }
    return order

def match_order(order, producer=None):
    """Try to match incoming order against book and produce trades when matched.

    Supports:
      - LIMIT orders: match at specified price or better
      - MARKET orders: match at best available price (any price)
      - IOC (Immediate-or-Cancel): fill what's available, cancel rest
      - FOK (Fill-or-Kill): fill entire quantity or reject

    Algorithm:
      - For BUY: match against lowest-price asks where ask.price <= buy.price (or any for market)
      - For SELL: match against highest-price bids where bid.price >= sell.price (or any for market)
    """
    symbol = order['symbol']
    if not symbol:
        return

    is_market = order.get('ord_type') == 'MARKET'
    tif = order.get('time_in_force', 'DAY')

    with lock:
        book = order_books[symbol]

        # For FOK orders, check if full quantity can be filled first
        if tif == 'FOK':
            available_qty = 0
            if order['side'] == 'BUY':
                for ask in book['asks']:
                    if is_market or (ask['price'] is not None and order['price'] is not None and ask['price'] <= order['price']):
                        available_qty += ask['quantity']
            else:
                for bid in book['bids']:
                    if is_market or (bid['price'] is not None and order['price'] is not None and bid['price'] >= order['price']):
                        available_qty += bid['quantity']
            if available_qty < order['quantity']:
                print(f"FOK order rejected: only {available_qty} available, need {order['quantity']}")
                return  # Reject FOK order

        if order['side'] == 'BUY':
            # try to match with asks (sorted by lowest price)
            asks = book['asks']
            asks.sort(key=lambda x: (x['price'] if x['price'] is not None else float('inf'), x['timestamp']))
            remaining = order['quantity']
            i = 0
            while remaining > 0 and i < len(asks):
                ask = asks[i]
                # Market orders match at any price; limit orders check price
                if not is_market:
                    if ask['price'] is None or order['price'] is None or ask['price'] > order['price']:
                        break
                # matchable
                traded_qty = min(remaining, ask['quantity'])
                trade_price = ask['price']
                trade = {
                    'symbol': symbol,
                    'price': trade_price,
                    'quantity': traded_qty,
                    'buy_id': order.get('cl_ord_id'),
                    'sell_id': ask.get('cl_ord_id'),
                    'timestamp': time.time()
                }
                trades_log.appendleft(trade)
                # Persist trade to database
                try:
                    save_trade(trade)
                except Exception as e:
                    print("DB save_trade error:", e)
                if producer:
                    try:
                        producer.send(Config.TRADES_TOPIC, trade)
                    except Exception as e:
                        print("Producer send error:", e)
                remaining -= traded_qty
                ask['quantity'] -= traded_qty
                # Update order quantity in database
                if ask.get('cl_ord_id'):
                    try:
                        update_order_quantity(ask['cl_ord_id'], ask['quantity'])
                    except Exception as e:
                        print("DB update_order error:", e)
                if ask['quantity'] == 0:
                    asks.pop(i)
                else:
                    i += 1
            # if remaining, add to bids and persist (unless market or IOC order)
            if remaining > 0:
                if is_market or tif == 'IOC':
                    # Market and IOC orders don't rest on book
                    print(f"{'Market' if is_market else 'IOC'} order cancelled: {remaining} unfilled")
                else:
                    new_order = dict(order)
                    new_order['quantity'] = remaining
                    book['bids'].append(new_order)
                    # Persist resting order to database
                    try:
                        save_order(new_order)
                    except Exception as e:
                        print("DB save_order error:", e)

        elif order['side'] == 'SELL':
            bids = book['bids']
            bids.sort(key=lambda x: (-x['price'] if x['price'] is not None else 0, x['timestamp']))
            remaining = order['quantity']
            i = 0
            while remaining > 0 and i < len(bids):
                bid = bids[i]
                # Market orders match at any price; limit orders check price
                if not is_market:
                    if bid['price'] is None or order['price'] is None or bid['price'] < order['price']:
                        break
                traded_qty = min(remaining, bid['quantity'])
                trade_price = bid['price']
                trade = {
                    'symbol': symbol,
                    'price': trade_price,
                    'quantity': traded_qty,
                    'buy_id': bid.get('cl_ord_id'),
                    'sell_id': order.get('cl_ord_id'),
                    'timestamp': time.time()
                }
                trades_log.appendleft(trade)
                # Persist trade to database
                try:
                    save_trade(trade)
                except Exception as e:
                    print("DB save_trade error:", e)
                if producer:
                    try:
                        producer.send(Config.TRADES_TOPIC, trade)
                    except Exception as e:
                        print("Producer send error:", e)
                remaining -= traded_qty
                bid['quantity'] -= traded_qty
                # Update order quantity in database
                if bid.get('cl_ord_id'):
                    try:
                        update_order_quantity(bid['cl_ord_id'], bid['quantity'])
                    except Exception as e:
                        print("DB update_order error:", e)
                if bid['quantity'] == 0:
                    bids.pop(i)
                else:
                    i += 1
            # if remaining, add to asks and persist (unless market or IOC order)
            if remaining > 0:
                if is_market or tif == 'IOC':
                    # Market and IOC orders don't rest on book
                    print(f"{'Market' if is_market else 'IOC'} order cancelled: {remaining} unfilled")
                else:
                    new_order = dict(order)
                    new_order['quantity'] = remaining
                    book['asks'].append(new_order)
                    # Persist resting order to database
                    try:
                        save_order(new_order)
                    except Exception as e:
                        print("DB save_order error:", e)
        else:
            # unknown side: append to book as-is
            order_books[symbol]['bids' if order.get('side','').upper()=='BUY' else 'asks'].append(order)

def handle_cancel(msg, producer=None):
    """Handle order cancellation request."""
    orig_cl_ord_id = msg.get('orig_cl_ord_id')
    symbol = msg.get('symbol')

    if not orig_cl_ord_id:
        print("Cancel rejected: missing orig_cl_ord_id")
        return

    with lock:
        found = False
        # Search all order books if symbol not specified
        symbols_to_search = [symbol] if symbol else list(order_books.keys())

        for sym in symbols_to_search:
            book = order_books.get(sym, {'bids': [], 'asks': []})

            # Search bids
            for i, order in enumerate(book['bids']):
                if order.get('cl_ord_id') == orig_cl_ord_id:
                    book['bids'].pop(i)
                    found = True
                    print(f"Cancelled BUY order {orig_cl_ord_id} in {sym}")
                    break

            if found:
                break

            # Search asks
            for i, order in enumerate(book['asks']):
                if order.get('cl_ord_id') == orig_cl_ord_id:
                    book['asks'].pop(i)
                    found = True
                    print(f"Cancelled SELL order {orig_cl_ord_id} in {sym}")
                    break

            if found:
                break

        if found:
            # Update database
            try:
                from database import cancel_order
                cancel_order(orig_cl_ord_id)
            except Exception as e:
                print(f"DB cancel_order error: {e}")
        else:
            print(f"Cancel rejected: order {orig_cl_ord_id} not found")


def handle_amend(msg, producer=None):
    """Handle order amend (cancel/replace) request."""
    orig_cl_ord_id = msg.get('orig_cl_ord_id')
    new_cl_ord_id = msg.get('cl_ord_id')
    symbol = msg.get('symbol')
    new_qty = msg.get('quantity')
    new_price = msg.get('price')

    if not orig_cl_ord_id:
        print("Amend rejected: missing orig_cl_ord_id")
        return

    with lock:
        found = False
        symbols_to_search = [symbol] if symbol else list(order_books.keys())

        for sym in symbols_to_search:
            book = order_books.get(sym, {'bids': [], 'asks': []})

            # Search bids
            for order in book['bids']:
                if order.get('cl_ord_id') == orig_cl_ord_id:
                    # Update order in place
                    if new_qty is not None and new_qty > 0:
                        order['quantity'] = new_qty
                    if new_price is not None and new_price > 0:
                        order['price'] = new_price
                    if new_cl_ord_id:
                        order['cl_ord_id'] = new_cl_ord_id
                    found = True
                    print(f"Amended BUY order {orig_cl_ord_id} -> qty={new_qty}, price={new_price}")
                    break

            if found:
                break

            # Search asks
            for order in book['asks']:
                if order.get('cl_ord_id') == orig_cl_ord_id:
                    if new_qty is not None and new_qty > 0:
                        order['quantity'] = new_qty
                    if new_price is not None and new_price > 0:
                        order['price'] = new_price
                    if new_cl_ord_id:
                        order['cl_ord_id'] = new_cl_ord_id
                    found = True
                    print(f"Amended SELL order {orig_cl_ord_id} -> qty={new_qty}, price={new_price}")
                    break

            if found:
                break

        if not found:
            print(f"Amend rejected: order {orig_cl_ord_id} not found")


def consume_orders():
    producer = None
    try:
        producer = create_producer(component_name="Matcher")
    except Exception as e:
        print("Producer unavailable:", e)

    try:
        consumer = create_consumer(
            topics=Config.ORDERS_TOPIC,
            group_id="matcher",
            component_name="Matcher"
        )
    except Exception as e:
        print("Consumer unavailable:", e)
        return

    for msg in consumer:
        try:
            raw = msg.value
        except Exception:
            continue

        # Handle special message types (cancel, amend)
        msg_type = raw.get('type', '').lower() if isinstance(raw, dict) else ''

        if msg_type == 'cancel':
            handle_cancel(raw, producer)
            continue
        elif msg_type == 'amend':
            handle_amend(raw, producer)
            continue

        # Regular order
        order = normalize_order(raw)
        if not order:
            continue
        print("Matcher received order:", order.get('symbol'), order.get('side'), order.get('quantity'), order.get('price'))
        match_order(order, producer=producer)

@app.route("/health")
def health():
    """Health check endpoint."""
    status = {
        "status": "healthy",
        "service": "matcher",
        "timestamp": time.time(),
        "stats": {
            "symbols": len(order_books),
            "total_orders": sum(len(b["bids"]) + len(b["asks"]) for b in order_books.values()),
            "trades_in_memory": len(trades_log)
        }
    }
    # Check database connectivity
    try:
        from database import get_trade_count
        status["stats"]["trades_in_db"] = get_trade_count()
        status["database"] = "connected"
    except Exception as e:
        status["database"] = f"error: {e}"
        status["status"] = "degraded"

    return jsonify(status)

@app.route("/trades")
def trades_endpoint():
    """Get trades with optional filtering and pagination.

    Query params:
        symbol: Filter by symbol (optional)
        limit: Max records to return (default 200)
        offset: Records to skip (default 0)
    """
    symbol = request.args.get('symbol')
    limit = request.args.get('limit', 200, type=int)
    offset = request.args.get('offset', 0, type=int)

    # Fetch from database for persistence
    trades = get_trades(symbol=symbol, limit=limit, offset=offset)
    total = get_trade_count(symbol=symbol)

    return jsonify({
        'trades': trades,
        'total': total,
        'limit': limit,
        'offset': offset
    })

@app.route("/orderbook/<symbol>")
def get_orderbook(symbol):
    with lock:
        book = order_books[symbol]
        bids = sorted([o for o in book["bids"]], key=lambda x: -float(x["price"]) if x.get("price") is not None else 0)
        asks = sorted([o for o in book["asks"]], key=lambda x: float(x["price"]) if x.get("price") is not None else float('inf'))
        return jsonify({"symbol": symbol, "bids": bids, "asks": asks})

# Metrics tracking
_metrics = {
    "orders_received": 0,
    "trades_executed": 0,
    "cancels_processed": 0,
    "amends_processed": 0,
    "start_time": time.time()
}

@app.route("/metrics")
def metrics():
    """Prometheus-compatible metrics endpoint."""
    with lock:
        total_bids = sum(len(b["bids"]) for b in order_books.values())
        total_asks = sum(len(b["asks"]) for b in order_books.values())

    try:
        db_trades = get_trade_count()
    except:
        db_trades = 0

    uptime = time.time() - _metrics["start_time"]

    # Prometheus text format
    lines = [
        "# HELP matcher_orders_received_total Total orders received",
        "# TYPE matcher_orders_received_total counter",
        f"matcher_orders_received_total {_metrics['orders_received']}",
        "",
        "# HELP matcher_trades_executed_total Total trades executed",
        "# TYPE matcher_trades_executed_total counter",
        f"matcher_trades_executed_total {db_trades}",
        "",
        "# HELP matcher_cancels_processed_total Total cancel requests processed",
        "# TYPE matcher_cancels_processed_total counter",
        f"matcher_cancels_processed_total {_metrics['cancels_processed']}",
        "",
        "# HELP matcher_amends_processed_total Total amend requests processed",
        "# TYPE matcher_amends_processed_total counter",
        f"matcher_amends_processed_total {_metrics['amends_processed']}",
        "",
        "# HELP matcher_order_book_bids Current number of bid orders",
        "# TYPE matcher_order_book_bids gauge",
        f"matcher_order_book_bids {total_bids}",
        "",
        "# HELP matcher_order_book_asks Current number of ask orders",
        "# TYPE matcher_order_book_asks gauge",
        f"matcher_order_book_asks {total_asks}",
        "",
        "# HELP matcher_symbols_active Number of active trading symbols",
        "# TYPE matcher_symbols_active gauge",
        f"matcher_symbols_active {len(order_books)}",
        "",
        "# HELP matcher_uptime_seconds Time since service started",
        "# TYPE matcher_uptime_seconds counter",
        f"matcher_uptime_seconds {uptime:.2f}",
        "",
    ]

    return "\n".join(lines), 200, {"Content-Type": "text/plain; charset=utf-8"}

if __name__ == "__main__":
    threading.Thread(target=consume_orders, daemon=True).start()
    app.run(host="0.0.0.0", port=6000, debug=True)
