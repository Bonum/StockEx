"""SQLite persistence layer for the matcher service."""

import sqlite3
import threading
import time
import os

DB_PATH = os.getenv("DB_PATH", "/app/data/matcher.db")

_local = threading.local()


def get_connection():
    """Get a thread-local database connection."""
    if not hasattr(_local, "conn") or _local.conn is None:
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        _local.conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        _local.conn.row_factory = sqlite3.Row
    return _local.conn


def init_db():
    """Initialize database schema."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.executescript("""
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            price REAL NOT NULL,
            quantity INTEGER NOT NULL,
            buy_order_id TEXT,
            sell_order_id TEXT,
            timestamp REAL NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS order_book (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cl_ord_id TEXT UNIQUE,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            price REAL,
            quantity INTEGER NOT NULL,
            remaining_qty INTEGER NOT NULL,
            status TEXT DEFAULT 'OPEN',
            timestamp REAL NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades(symbol);
        CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades(timestamp);
        CREATE INDEX IF NOT EXISTS idx_orderbook_symbol_side ON order_book(symbol, side);
        CREATE INDEX IF NOT EXISTS idx_orderbook_status ON order_book(status);
        CREATE INDEX IF NOT EXISTS idx_orderbook_cl_ord_id ON order_book(cl_ord_id);
    """)

    conn.commit()
    print(f"Database initialized at {DB_PATH}")


def save_trade(trade: dict):
    """Persist a trade to the database."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO trades (symbol, price, quantity, buy_order_id, sell_order_id, timestamp)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (
        trade.get("symbol"),
        trade.get("price"),
        trade.get("quantity"),
        trade.get("buy_id"),
        trade.get("sell_id"),
        trade.get("timestamp", time.time())
    ))
    conn.commit()
    return cursor.lastrowid


def get_trades(symbol: str = None, limit: int = 200, offset: int = 0):
    """Retrieve trades from database."""
    conn = get_connection()
    cursor = conn.cursor()

    if symbol:
        cursor.execute("""
            SELECT * FROM trades
            WHERE symbol = ?
            ORDER BY timestamp DESC
            LIMIT ? OFFSET ?
        """, (symbol, limit, offset))
    else:
        cursor.execute("""
            SELECT * FROM trades
            ORDER BY timestamp DESC
            LIMIT ? OFFSET ?
        """, (limit, offset))

    rows = cursor.fetchall()
    return [dict(row) for row in rows]


def get_trade_count(symbol: str = None):
    """Get total trade count."""
    conn = get_connection()
    cursor = conn.cursor()

    if symbol:
        cursor.execute("SELECT COUNT(*) FROM trades WHERE symbol = ?", (symbol,))
    else:
        cursor.execute("SELECT COUNT(*) FROM trades")

    return cursor.fetchone()[0]


def save_order(order: dict):
    """Save or update an order in the order book."""
    conn = get_connection()
    cursor = conn.cursor()

    cl_ord_id = order.get("cl_ord_id")
    if not cl_ord_id:
        cl_ord_id = f"gen-{time.time_ns()}"

    cursor.execute("""
        INSERT INTO order_book (cl_ord_id, symbol, side, price, quantity, remaining_qty, status, timestamp)
        VALUES (?, ?, ?, ?, ?, ?, 'OPEN', ?)
        ON CONFLICT(cl_ord_id) DO UPDATE SET
            remaining_qty = excluded.remaining_qty,
            status = CASE WHEN excluded.remaining_qty = 0 THEN 'FILLED' ELSE status END
    """, (
        cl_ord_id,
        order.get("symbol"),
        order.get("side"),
        order.get("price"),
        order.get("quantity"),
        order.get("quantity"),  # remaining_qty starts as full quantity
        order.get("timestamp", time.time())
    ))
    conn.commit()
    return cl_ord_id


def update_order_quantity(cl_ord_id: str, remaining_qty: int):
    """Update remaining quantity for an order."""
    conn = get_connection()
    cursor = conn.cursor()

    status = "FILLED" if remaining_qty == 0 else "OPEN"
    cursor.execute("""
        UPDATE order_book
        SET remaining_qty = ?, status = ?
        WHERE cl_ord_id = ?
    """, (remaining_qty, status, cl_ord_id))
    conn.commit()


def cancel_order(cl_ord_id: str):
    """Mark an order as cancelled."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE order_book SET status = 'CANCELLED' WHERE cl_ord_id = ?
    """, (cl_ord_id,))
    conn.commit()
    return cursor.rowcount > 0


def get_open_orders(symbol: str = None, side: str = None):
    """Get all open orders, optionally filtered by symbol and side."""
    conn = get_connection()
    cursor = conn.cursor()

    query = "SELECT * FROM order_book WHERE status = 'OPEN'"
    params = []

    if symbol:
        query += " AND symbol = ?"
        params.append(symbol)
    if side:
        query += " AND side = ?"
        params.append(side)

    query += " ORDER BY timestamp ASC"
    cursor.execute(query, params)

    rows = cursor.fetchall()
    return [dict(row) for row in rows]


def load_order_books():
    """Load all open orders grouped by symbol for matcher initialization."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT * FROM order_book
        WHERE status = 'OPEN'
        ORDER BY symbol, side, timestamp
    """)

    books = {}
    for row in cursor.fetchall():
        order = dict(row)
        symbol = order["symbol"]
        side = order["side"]

        if symbol not in books:
            books[symbol] = {"bids": [], "asks": []}

        # Convert DB format to matcher format
        matcher_order = {
            "cl_ord_id": order["cl_ord_id"],
            "symbol": symbol,
            "side": side,
            "price": order["price"],
            "quantity": order["remaining_qty"],
            "timestamp": order["timestamp"]
        }

        if side == "BUY":
            books[symbol]["bids"].append(matcher_order)
        else:
            books[symbol]["asks"].append(matcher_order)

    return books


def delete_filled_orders(older_than_days: int = 7):
    """Clean up old filled orders."""
    conn = get_connection()
    cursor = conn.cursor()
    cutoff = time.time() - (older_than_days * 24 * 60 * 60)
    cursor.execute("""
        DELETE FROM order_book
        WHERE status IN ('FILLED', 'CANCELLED')
        AND timestamp < ?
    """, (cutoff,))
    conn.commit()
    return cursor.rowcount
